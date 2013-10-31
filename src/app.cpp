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

#include "cocaine/app.hpp"

#include "cocaine/api/driver.hpp"
#include "cocaine/api/event.hpp"
#include "cocaine/api/isolate.hpp"

#include "cocaine/asio/acceptor.hpp"
#include "cocaine/asio/reactor.hpp"
#include "cocaine/asio/local.hpp"
#include "cocaine/asio/socket.hpp"

#include "cocaine/context.hpp"

#include "cocaine/detail/actor.hpp"
#include "cocaine/detail/engine.hpp"
#include "cocaine/detail/manifest.hpp"
#include "cocaine/detail/profile.hpp"

#include "cocaine/dispatch.hpp"
#include "cocaine/logging.hpp"
#include "cocaine/memory.hpp"
#include "cocaine/messages.hpp"

#include "cocaine/rpc/channel.hpp"

#include "cocaine/traits/dynamic.hpp"

#include <tuple>

#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

using namespace cocaine;
using namespace cocaine::engine;
using namespace cocaine::io;

namespace fs = boost::filesystem;

struct app_t::service_t:
    public implementation<io::app_tag>
{
    struct streaming_service_t:
        public implementation<io::streaming_tag>
    {
        streaming_service_t(context_t& context, const std::string& name, const api::stream_ptr_t& d):
            implementation<io::streaming_tag>(context, name),
            downstream(d)
        { }

        void
        write(const std::string& chunk) {
            downstream->write(chunk.data(), chunk.size());
        }

        void
        close() {
            downstream->close();
        }

    private:
        const api::stream_ptr_t downstream;
    };

    struct enqueue_slot_t:
        public basic_slot<io::app::enqueue>
    {
        enqueue_slot_t(app_t::service_t& self_):
            self(self_)
        { }

        virtual
        std::shared_ptr<dispatch_t>
        operator()(const msgpack::object& unpacked, const api::stream_ptr_t& upstream) {
            return io::detail::invoke<event_traits<app::enqueue>::tuple_type>::apply(
                boost::bind(&service_t::enqueue, &self, upstream, _1, _2),
                unpacked
            );
        }

    private:
        app_t::service_t& self;
    };

    struct write_slot_t:
        public basic_slot<io::streaming::write>
    {
        write_slot_t(const std::shared_ptr<streaming_service_t>& self_):
            self(self_)
        { }

        virtual
        std::shared_ptr<dispatch_t>
        operator()(const msgpack::object& unpacked, const api::stream_ptr_t& /* upstream */) {
            io::detail::invoke<event_traits<rpc::chunk>::tuple_type>::apply(
                boost::bind(&streaming_service_t::write, self.get(), _1),
                unpacked
            );

            return self;
        }

    private:
        const std::shared_ptr<streaming_service_t> self;
    };

    service_t(context_t& context_, const std::string& name_, app_t& app_):
        implementation<io::app_tag>(context_, cocaine::format("service/%1%", name_)),
        context(context_),
        app(app_)
    {
        on<io::app::enqueue>(std::make_shared<enqueue_slot_t>(*this));
        on<io::app::info>(std::bind(&app_t::info, std::ref(app)));
    }

private:
    std::shared_ptr<dispatch_t>
    enqueue(const api::stream_ptr_t& upstream, const std::string& event, const std::string& tag) {
        api::stream_ptr_t downstream;

        if(tag.empty()) {
            downstream = app.enqueue(api::event_t(event), upstream);
        } else {
            downstream = app.enqueue(api::event_t(event), upstream, tag);
        }

        auto service = std::make_shared<streaming_service_t>(context, name(), downstream);

        service->on<io::streaming::write>(std::make_shared<write_slot_t>(service));
        service->on<io::streaming::close>(std::bind(&streaming_service_t::close, service));

        return service;
    }

private:
    context_t& context;
    app_t& app;
};

app_t::app_t(context_t& context, const std::string& name, const std::string& profile):
    m_context(context),
    m_log(new logging::log_t(context, cocaine::format("app/%1%", name))),
    m_manifest(new manifest_t(context, name)),
    m_profile(new profile_t(context, profile))
{
    auto isolate = m_context.get<api::isolate_t>(
        m_profile->isolate.type,
        m_context,
        m_manifest->name,
        m_profile->isolate.args
    );

    if(m_manifest->source() != sources::cache) {
        isolate->spool();
    }

    std::shared_ptr<io::socket<local>> lhs, rhs;

    // Create the engine control sockets.
    std::tie(lhs, rhs) = io::link<local>();

    m_reactor = std::make_unique<reactor_t>();
    m_engine_control = std::make_unique<channel<io::socket<local>>>(*m_reactor, lhs);

    try {
        m_engine.reset(new engine_t(m_context, std::make_shared<reactor_t>(), *m_manifest, *m_profile, rhs));
    } catch(const std::system_error& e) {
        throw cocaine::error_t(
            "unable to initialize the engine - %s - [%d] %s",
            e.what(),
            e.code().value(),
            e.code().message()
        );
    }
}

app_t::~app_t() {
    // Empty.
}

void
app_t::start() {
    COCAINE_LOG_INFO(m_log, "starting the engine");

    auto drivers = driver_map_t();

    if(!m_manifest->drivers.empty()) {
        COCAINE_LOG_DEBUG(
            m_log,
            "starting %llu %s",
            m_manifest->drivers.size(),
            m_manifest->drivers.size() == 1 ? "driver" : "drivers"
        );

        api::category_traits<api::driver_t>::ptr_type driver;

        for(auto it = m_manifest->drivers.begin(); it != m_manifest->drivers.end(); ++it) {
            const std::string name = cocaine::format(
                "%s/%s",
                m_manifest->name,
                it->first
            );

            COCAINE_LOG_DEBUG(m_log, "starting driver '%s', type: %s", it->first, it->second.type);

            try {
                driver = m_context.get<api::driver_t>(
                    it->second.type,
                    m_context,
                    m_engine->reactor(),
                    *this,
                    name,
                    it->second.args
                );
            } catch(const cocaine::error_t& e) {
                throw cocaine::error_t("unable to initialize the '%s' driver - %s", name, e.what());
            } catch(...) {
                throw cocaine::error_t("unable to initialize the '%s' driver - unknown exception", name);
            }

            drivers[it->first] = std::move(driver);
        }
    }

    // We can safely swap the current driver set now.
    m_drivers.swap(drivers);

    // Start the engine thread.
    m_thread.reset(new std::thread(std::bind(&engine_t::run, m_engine)));

    COCAINE_LOG_DEBUG(m_log, "starting the invocation service");

    // Publish the app service.
    m_context.attach(m_manifest->name, std::make_unique<actor_t>(
        m_context,
        std::make_shared<reactor_t>(),
        std::unique_ptr<dispatch_t>(new app_t::service_t(m_context, m_manifest->name, *this))
    ));

    COCAINE_LOG_INFO(m_log, "the engine has started");
}

namespace {
    namespace detail {
        template<class It, class End, typename... Args>
        struct fold_impl {
            typedef typename fold_impl<
                typename boost::mpl::next<It>::type,
                End,
                Args...,
                typename std::add_lvalue_reference<
                    typename boost::mpl::deref<It>::type
                >::type
            >::type type;
        };

        template<class End, typename... Args>
        struct fold_impl<End, End, Args...> {
            typedef std::tuple<Args...> type;
        };

        template<class TupleType, int N = std::tuple_size<TupleType>::value>
        struct unfold_impl {
            template<class Event, typename... Args>
            static inline
            void
            apply(const message_t& message,
                  TupleType& tuple,
                  Args&&... args)
            {
                unfold_impl<TupleType, N - 1>::template apply<Event>(
                    message,
                    tuple,
                    std::get<N - 1>(tuple),
                    std::forward<Args>(args)...
                );
            }
        };

        template<class TupleType>
        struct unfold_impl<TupleType, 0> {
            template<class Event, typename... Args>
            static inline
            void
            apply(const message_t& message,
                  TupleType& /* tuple */,
                  Args&&... args)
            {
                message.as<Event>(std::forward<Args>(args)...);
            }
        };
    }

    template<class TypeList>
    struct fold {
        typedef typename detail::fold_impl<
            typename boost::mpl::begin<TypeList>::type,
            typename boost::mpl::end<TypeList>::type
        >::type type;
    };

    template<class TupleType>
    struct unfold {
        template<class Event>
        static inline
        void
        apply(const message_t& message,
              TupleType& tuple)
        {
            return detail::unfold_impl<
                TupleType
            >::template apply<Event>(message, tuple);
        }
    };

    template<class Event>
    struct expect {
        template<class>
        struct result {
            typedef void type;
        };

        template<typename... Args>
        expect(reactor_t& reactor, Args&&... args):
            m_reactor(reactor),
            m_tuple(std::forward<Args>(args)...)
        { }

        expect(expect&& other):
            m_reactor(other.m_reactor),
            m_tuple(std::move(other.m_tuple))
        { }

        expect&
        operator=(expect&& other) {
            m_tuple = std::move(other.m_tuple);
            return *this;
        }

        void
        operator()(const message_t& message) {
            if(message.id() == event_traits<Event>::id) {
                unfold<tuple_type>::template apply<Event>(
                    message,
                    m_tuple
                );

                m_reactor.stop();
            }
        }

        void
        operator()(const std::error_code& ec) {
            throw cocaine::error_t("i/o failure — [%d] %s", ec.value(), ec.message());
        }

    private:
        typedef typename fold<
            typename event_traits<Event>::tuple_type
        >::type tuple_type;

        reactor_t& m_reactor;
        tuple_type m_tuple;
    };
}

void
app_t::stop() {
    COCAINE_LOG_INFO(m_log, "stopping the engine");

    if(!m_manifest->local) {
        // Destroy the app service.
        m_context.detach(m_manifest->name);
    }

    auto callback = expect<control::terminate>(*m_reactor);

    m_engine_control->rd->bind(std::ref(callback), std::ref(callback));
    m_engine_control->wr->write<control::terminate>(0UL);

    // Blocks until the engine is stopped.
    m_reactor->run();

    m_thread->join();
    m_thread.reset();

    // NOTE: Stop the drivers, so that there won't be any open
    // sockets and so on while the engine is stopped.
    m_drivers.clear();

    // NOTE: Destroy the engine last, because it holds the only
    // reference to the reactor which drivers use.
    m_engine.reset();

    COCAINE_LOG_INFO(m_log, "the engine has stopped");
}

dynamic_t
app_t::info() const {
    dynamic_t info = dynamic_t::object_t();

    if(!m_thread) {
        info.as_object()["error"] = "the engine is not active";
        return info;
    }

    auto callback = expect<control::info>(*m_reactor, info);

    m_engine_control->rd->bind(std::ref(callback), std::ref(callback));
    m_engine_control->wr->write<control::report>(0UL);

    try {
        // Blocks until either the response or timeout happens.
        m_reactor->run_with_timeout(defaults::control_timeout);
    } catch(const cocaine::error_t& e) {
        info.as_object()["error"] = "the engine is unresponsive";
        return info;
    }

    info.as_object()["profile"] = m_profile->name;

    dynamic_t::object_t drivers;
    for(auto it = m_drivers.begin(); it != m_drivers.end(); ++it) {
        drivers[it->first] = it->second->info();
    }
    info.as_object()["drivers"] = drivers;

    return info;
}

std::shared_ptr<api::stream_t>
app_t::enqueue(const api::event_t& event, const std::shared_ptr<api::stream_t>& upstream) {
    return m_engine->enqueue(event, upstream);
}

std::shared_ptr<api::stream_t>
app_t::enqueue(const api::event_t& event, const std::shared_ptr<api::stream_t>& upstream, const std::string& tag) {
    return m_engine->enqueue(event, upstream, tag);
}
