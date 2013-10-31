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

#include "cocaine/detail/services/node.hpp"

#include "cocaine/api/storage.hpp"

#include "cocaine/app.hpp"
#include "cocaine/context.hpp"
#include "cocaine/logging.hpp"
#include "cocaine/messages.hpp"

#include "cocaine/traits/dynamic.hpp"

#include <tuple>

using namespace cocaine;
using namespace cocaine::service;
using namespace std::placeholders;

namespace {

typedef std::map<std::string, std::string> runlist_t;

}

node_t::node_t(context_t& context, io::reactor_t& reactor, const std::string& name, const dynamic_t& args):
    api::service_t(context, reactor, name, args),
    implementation<io::node_tag>(context, name),
    m_context(context),
    m_log(new logging::log_t(context, name))
{
    on<io::node::start_app>(std::bind(&node_t::on_start_app, this, _1));
    on<io::node::pause_app>(std::bind(&node_t::on_pause_app, this, _1));
    on<io::node::list>(std::bind(&node_t::on_list, this));

    const auto runlist_id = args.as_object().at("runlist", "default").as_string();

    // It's here to keep the reference alive.
    const auto storage = api::storage(m_context, "core");

    runlist_t runlist;

    COCAINE_LOG_INFO(m_log, "reading the '%s' runlist", runlist_id);

    try {
        runlist = storage->get<runlist_t>("runlists", runlist_id);
    } catch(const storage_error_t& e) {
        COCAINE_LOG_WARNING(m_log, "unable to read the '%s' runlist - %s", runlist_id, e.what());
    }

    if(!runlist.empty()) {
        COCAINE_LOG_INFO(m_log, "starting %d %s", runlist.size(), runlist.size() == 1 ? "app" : "apps");

        // NOTE: Ignore the return value here, as there's nowhere to return it. It might be nice to
        // parse and log it in case of errors or simply die.
        on_start_app(runlist);
    }
}

node_t::~node_t() {
    if(m_apps.empty()) {
        return;
    }

    COCAINE_LOG_INFO(m_log, "stopping the apps");

    for(auto it = m_apps.begin(); it != m_apps.end(); ++it) {
        it->second->stop();
    }

    m_apps.clear();
}

dynamic_t
node_t::on_start_app(const runlist_t& runlist) {
    dynamic_t::object_t result;

    for(auto it = runlist.begin(); it != runlist.end(); ++it) {
        if(m_apps.find(it->first) != m_apps.end()) {
            result[it->first] = "the app is already running";
            continue;
        }

        COCAINE_LOG_INFO(m_log, "starting the '%s' app", it->first);

        app_map_t::iterator app;

        try {
            std::tie(app, std::ignore) = m_apps.insert({
                it->first,
                std::make_shared<app_t>(m_context, it->first, it->second)
            });
        } catch(const cocaine::error_t& e) {
            COCAINE_LOG_ERROR(m_log, "unable to initialize the '%s' app - %s", it->first, e.what());
            result[it->first] = std::string(e.what());
            continue;
        }

        try {
            app->second->start();
        } catch(const cocaine::error_t& e) {
            COCAINE_LOG_ERROR(m_log, "unable to start the '%s' app - %s", it->first, e.what());
            m_apps.erase(app);
            result[it->first] = std::string(e.what());
            continue;
        }

        result[it->first] = "the app has been started";
    }

    return std::move(result);
}

dynamic_t
node_t::on_pause_app(const std::vector<std::string>& applist) {
    dynamic_t::object_t result;

    for(auto it = applist.begin(); it != applist.end(); ++it) {
        auto app = m_apps.find(*it);

        if(app == m_apps.end()) {
            result[*it] = "the app is not running";
            continue;
        }

        COCAINE_LOG_INFO(m_log, "stopping the '%s' app", *it);

        app->second->stop();
        m_apps.erase(app);

        result[*it] = "the app has been stopped";
    }

    return std::move(result);
}

dynamic_t
node_t::on_list() const {
    dynamic_t::array_t result;

    for(auto it = m_apps.begin(); it != m_apps.end(); ++it) {
        result.push_back(it->first);
    }

    return std::move(result);
}
