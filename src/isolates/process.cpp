/*
    Copyright (c) 2011-2013 Andrey Sibiryov <me@kobology.ru>
    Copyright (c) 2011-2013 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "cocaine/detail/isolates/process.hpp"

#include "cocaine/context.hpp"
#include "cocaine/logging.hpp"
#include "cocaine/memory.hpp"

#include <array>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <system_error>
#include <iostream>

#include <boost/filesystem/operations.hpp>

#ifdef COCAINE_ALLOW_CGROUPS
#include <boost/lexical_cast.hpp>
#endif

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifdef COCAINE_ALLOW_CGROUPS
#include <libcgroup.h>
#endif

using namespace cocaine;
using namespace cocaine::isolate;

namespace fs = boost::filesystem;

namespace {

struct process_handle_t:
    public api::handle_t
{
    process_handle_t(pid_t pid, int stdout):
        m_pid(pid),
        m_stdout(stdout)
    { }

    virtual
   ~process_handle_t() {
        terminate();
    }

    virtual
    void
    terminate() {
        int status = 0;

        if(::waitpid(m_pid, &status, WNOHANG) == 0) {
            ::kill(m_pid, SIGTERM);
        }

        ::close(m_stdout);
    }

    virtual
    int
    stdout() const {
        return m_stdout;
    }

private:
    const pid_t m_pid;
    const int m_stdout;
};

#ifdef COCAINE_ALLOW_CGROUPS
struct cgroup_configurator :
    public boost::static_visitor<>
{
    cgroup_configurator(cgroup_controller *ctl,
                        const char *name,
                        const char *par,
                        const std::shared_ptr<logging::log_t> &log) :
        m_ctl(ctl),
        m_name(name),
        m_par(par),
        m_log(log),
    {
        // pass
    }

    void
    operator()(const dynamic_t::bool_t& v) const {
        cgroup_add_value_bool(m_ctl, m_par, v);
    }

    void
    operator()(const dynamic_t::int_t& v) const {
        cgroup_add_value_int64(m_ctl, m_par, v);
    }

    void
    operator()(const dynamic_t::uint_t& v) const {
        cgroup_add_value_uint64(m_ctl, m_par, v);
    }

    void
    operator()(const dynamic_t::string_t& v) const {
        cgroup_add_value_string(m_ctl, m_par, v.c_str())
    }

    template<class T>
    void
    operator()(const T& v) const {
        COCAINE_LOG_WARNING(m_log, "cgroup controller '%s' parameter '%s' type is not supported", m_name, m_par);
    }

private:
    cgroup_controller *m_ctl;
    const char *m_name;
    const char *m_par;
    const std::shared_ptr<logging::log_t> &m_log;
};
#endif

}

process_t::process_t(context_t& context, const std::string& name, const dynamic_t& args):
    category_type(context, name, args),
    m_context(context),
    m_log(new logging::log_t(context, name)),
    m_name(name),
    m_working_directory(fs::path(args.as_object().at("spool", "/var/spool/cocaine").as_string()) / name)
{
#ifdef COCAINE_ALLOW_CGROUPS
    int rv = 0;

    if((rv = cgroup_init()) != 0) {
        throw cocaine::error_t("unable to initialize the cgroups isolate - %s", cgroup_strerror(rv));
    }

    m_cgroup = cgroup_new_cgroup(name.c_str());

    // TODO: Check if it changes anything.
    cgroup_set_uid_gid(m_cgroup, getuid(), getgid(), getuid(), getgid());

    for(auto c = args.as_object().begin(); c != args.as_object().end(); ++c) {
        if(!c->second.is_object() || c->second.as_object().empty()) {
            continue;
        }

        cgroup_controller* ctl = cgroup_add_controller(m_cgroup, c->first.c_str());

        for(auto p = c->second.as_object().begin(); p != c->second.as_object().end(); ++p) {
            p->second.apply(cgroup_configurator(ctl, c->first.c_str(), p->first.c_str(), m_log));

            COCAINE_LOG_DEBUG(
                m_log,
                "setting cgroup controller '%s' parameter '%s' to '%s'",
                c->first,
                p->first,
                boost::lexical_cast<std::string>(p->second)
            );
        }
    }

    if((rv = cgroup_create_cgroup(m_cgroup, false)) != 0) {
        cgroup_free(&m_cgroup);
        throw cocaine::error_t("unable to create the cgroup - %s", cgroup_strerror(rv));
    }
#endif
}

process_t::~process_t() {
#ifdef COCAINE_ALLOW_CGROUPS
    int rv = 0;

    if((rv = cgroup_delete_cgroup(m_cgroup, false)) != 0) {
        COCAINE_LOG_ERROR(
            m_log,
            "unable to delete the cgroup - %s",
            cgroup_strerror(rv)
        );
    }

    cgroup_free(&m_cgroup);
#endif
}

#ifdef __APPLE__
    #include <crt_externs.h>
    #define environ (*_NSGetEnviron())
#else
    extern char** environ;
#endif

std::unique_ptr<api::handle_t>
process_t::spawn(const std::string& path, const api::string_map_t& args, const api::string_map_t& environment) {
    std::array<int, 2> pipes;

    if(::pipe(pipes.data()) != 0) {
        throw std::system_error(errno, std::system_category(), "unable to create an output pipe");
    }

    for(auto it = pipes.begin(); it != pipes.end(); ++it) {
        ::fcntl(*it, F_SETFD, FD_CLOEXEC);
    }

    const pid_t pid = ::fork();

    if(pid < 0) {
        std::for_each(pipes.begin(), pipes.end(), ::close);
        throw std::system_error(errno, std::system_category(), "unable to fork");
    }

    ::close(pipes[pid > 0]);

    if(pid > 0) {
        return std::make_unique<process_handle_t>(pid, pipes[0]);
    }

    // Child initialization

    ::dup2(pipes[1], STDOUT_FILENO);
    ::dup2(pipes[1], STDERR_FILENO);

#ifdef COCAINE_ALLOW_CGROUPS
    // Attach to the control group

    int rv = 0;

    if((rv = cgroup_attach_task(m_cgroup)) != 0) {
        std::cerr << cocaine::format("unable to attach the process to a cgroup - %s", cgroup_strerror(rv));
        std::_Exit(EXIT_FAILURE);
    }
#endif

    // Set the correct working directory

    try {
        fs::current_path(m_working_directory);
    } catch(const fs::filesystem_error& e) {
        std::cerr << cocaine::format("unable to change the working directory to '%s' - %s", m_working_directory, e.what());
        std::_Exit(EXIT_FAILURE);
    }

    // Prepare the command line and the environment

    auto target = fs::path(path);

#if BOOST_VERSION >= 104600
    if(!target.is_absolute()) {
#else
    if(!target.is_complete()) {
#endif
        target = m_working_directory / target;
    }

#if BOOST_VERSION >= 104600
    std::vector<char*> argv = { ::strdup(target.native().c_str()) }, envp;
#else
    std::vector<char*> argv = { ::strdup(target.string().c_str()) }, envp;
#endif

    for(auto it = args.begin(); it != args.end(); ++it) {
        argv.push_back(::strdup(it->first.c_str()));
        argv.push_back(::strdup(it->second.c_str()));
    }

    argv.push_back(nullptr);

    for(char** ptr = environ; *ptr != nullptr; ++ptr) {
        envp.push_back(::strdup(*ptr));
    }

    boost::format format("%s=%s");

    for(auto it = environment.begin(); it != environment.end(); ++it, format.clear()) {
        envp.push_back(::strdup((format % it->first % it->second).str().c_str()));
    }

    envp.push_back(nullptr);

    // Unblock all the signals

    sigset_t signals;

    sigfillset(&signals);

    ::sigprocmask(SIG_UNBLOCK, &signals, nullptr);

    // Spawn the slave

    if(::execve(argv[0], argv.data(), envp.data()) != 0) {
        std::error_code ec(errno, std::system_category());
        std::cerr << cocaine::format("unable to execute '%s' - [%d] %s", path, ec.value(), ec.message());
    }

    std::_Exit(EXIT_FAILURE);
}
