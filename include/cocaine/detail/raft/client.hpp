/*
    Copyright (c) 2014-2014 Andrey Goryachev <andrey.goryachev@gmail.com>
    Copyright (c) 2011-2014 Other contributors as noted in the AUTHORS file.

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

#ifndef COCAINE_RAFT_CLIENT_HPP
#define COCAINE_RAFT_CLIENT_HPP

#include "cocaine/detail/raft/forwards.hpp"

#include "cocaine/api/connect.hpp"
#include "cocaine/logging.hpp"
#include "cocaine/context.hpp"
#include "cocaine/tuple.hpp"

#include <boost/asio.hpp>

namespace cocaine { namespace raft {

namespace detail {

template<class Drain>
struct event_result { };

template<class T>
struct event_result<cocaine::io::streaming_tag<T>> {
    typedef typename cocaine::tuple::fold<T>::type tuple_type;
    typedef typename std::tuple_element<0, tuple_type>::type type;
};

template<class... Args>
struct event_result<cocaine::io::streaming_tag<std::tuple<Args...>>> {
    typedef std::tuple<Args...> tuple_type;
    typedef typename std::tuple_element<0, tuple_type>::type type;
};

} // namespace detail

template<class Tag>
class disposable_client {
    template<class Result>
    class response_handler :
        public dispatch<typename io::stream_of<Result>::tag>
    {
        typedef typename io::protocol<typename io::stream_of<Result>::tag>::scope protocol;

    public:
        response_handler(disposable_client &client,
                         const std::function<void(const Result&)>& callback):
            dispatch<typename io::stream_of<Result>::tag>(""),
            m_client(client),
            m_callback(callback),
            m_cancelled(false)
        {
            this->template on<typename protocol::chunk>(
                std::bind(&response_handler::on_write, this, std::placeholders::_1)
            );
            this->template on<typename protocol::error>(std::bind(&response_handler::on_error, this));
            this->template on<typename protocol::choke>(std::bind(&response_handler::on_error, this));
        }

        ~response_handler() {
            on_error();
        }

    private:
        void
        on_write(const Result& response) {
            cancel();

            auto ec = response.error();

            if(!ec) {
                // Entry has been successfully committed to the state machine.
                m_client.reset();
                m_callback(response);
            } else if(ec == raft_errc::not_leader || ec == raft_errc::unknown) {
                // Connect to leader received from the remote or try the next remote.
                m_client.reset();

                if(m_client.m_redirection_counter < m_client.m_follow_redirect &&
                   response.leader() != node_id_t())
                {
                    ++m_client.m_redirection_counter;
                    m_client.ensure_connection(response.leader());
                } else {
                    m_client.try_next_remote();
                }
            } else {
                // The state machine is busy (some configuration change is in progress).
                // Retry after request timeout.
                m_client.reset_request();
                m_client.m_retry_timer.expires_from_now(boost::posix_time::milliseconds(m_client.m_request_timeout));
                m_client.m_retry_timer.async_wait(std::bind(&disposable_client::resend_request, &m_client, std::placeholders::_1));
            }
        }

        void
        on_error() {
            if(!cancelled()) {
                cancel();
                m_client.reset();
                m_client.try_next_remote();
            }
        }

        virtual
        void
        discard(const boost::system::error_code& COCAINE_UNUSED_(ec)) const {
            const_cast<response_handler*>(this)->on_error();
        }

        bool
        cancelled() const {
            return m_cancelled;
        }

        void
        cancel() {
            m_cancelled = true;
        }

    private:
        disposable_client &m_client;
        std::function<void(const Result&)> m_callback;
        bool m_cancelled;
    };

public:
    disposable_client(
        context_t &context,
        boost::asio::io_service& asio,
        const std::string& name,
        const std::vector<node_id_t>& remotes,
        // Probably there is no sense to do more then 2 jumps,
        // because usually most of nodes know where is leader.
        unsigned int follow_redirect = 2,
        // Boost optional is needed to provide default value dependent on the context.
        boost::optional<float> request_timeout = boost::none
    ):
        m_context(context),
        m_asio(asio),
        m_logger(context.log("raft_client/" + name)),
        m_timeout_timer(asio),
        m_retry_timer(asio),
        m_name(name),
        m_follow_redirect(follow_redirect),
        m_request_timeout(0),
        m_remotes(remotes),
        m_next_remote(0),
        m_redirection_counter(0)
    {
        if(request_timeout) {
            m_request_timeout = *request_timeout * 1000;
        } else {
            // Currently this client is used to configuration changes.
            // One configuration change requires 5-7 roundtrips,
            // so 6 election timeouts is a reasonable value for configuration change timeout.
            m_request_timeout = 6 * context.raft().options().election_timeout;
        }
    }

    ~disposable_client() {
        reset();
    }

    template<class Event, class ResultHandler, class ErrorHandler, class... Args>
    void
    call(const ResultHandler& result_handler,
         const ErrorHandler& error_handler,
         Args&&... args)
    {
        typedef typename detail::event_result<typename io::event_traits<Event>::upstream_type>::type
                result_type;

        m_error_handler = error_handler;

        m_request_sender = std::bind(&disposable_client::send_request<Event, result_type>,
                                     this,
                                     std::function<void(const result_type&)>(result_handler),
                                     io::aux::make_frozen<Event>(std::forward<Args>(args)...));

        try_next_remote();
    }

private:
    void
    reset() {
        reset_request();

        m_client.reset();
        m_resolver.reset();
    }

    void
    reset_request() {
        m_cancellation.cancel();

        m_timeout_timer.cancel();
        m_retry_timer.cancel();

        if(m_current_request) {
            m_current_request->drop();
            m_current_request.reset();
        }
    }

    template<class Event, class Result>
    struct client_caller {
        typedef io::upstream_ptr_t result_type;

        const std::unique_ptr<api::client<Tag>> &m_client;
        std::shared_ptr<dispatch<typename io::stream_of<Result>::tag>> m_handler;

        template<class... Args>
        io::upstream_ptr_t
        operator()(Args&&... args) const {
            return m_client->template invoke<Event>(m_handler, std::forward<Args>(args)...).basic();
        }
    };

    template<class Event, class Result>
    void
    send_request(const std::function<void(const Result&)>& callback,
                 const io::aux::frozen<Event>& event)
    {
        auto dispatch = std::make_shared<response_handler<Result>>(*this, callback);

        m_current_request = tuple::invoke(client_caller<Event, Result> {m_client, dispatch}, event.tuple);
    }

    void
    try_next_remote() {
        if(m_next_remote >= m_remotes.size()) {
            reset();
            m_error_handler();
            return;
        }

        const auto &endpoint = m_remotes[m_next_remote];

        if(!m_client) {
            m_next_remote = m_next_remote + 1;
        }

        m_redirection_counter = 0;

        ensure_connection(endpoint);
    }

    void
    ensure_connection(const node_id_t& endpoint) {
        if(m_current_request || m_resolver) {
            return;
        }

        m_timeout_timer.expires_from_now(boost::posix_time::milliseconds(m_request_timeout));
        m_timeout_timer.async_wait(m_cancellation.wrap(
            std::bind(&disposable_client::on_timeout, this, std::placeholders::_1)
        ));

        if(m_client) {
            m_request_sender();
        } else {
            COCAINE_LOG_DEBUG(m_logger,
                              "raft client is not connected, connecting to %s:%d",
                              endpoint.first,
                              endpoint.second)
            (blackhole::attribute::list({
                {"host", endpoint.first},
                {"port", endpoint.second}
            }));

            m_client.reset(new api::client<Tag>);

            boost::asio::ip::tcp::resolver resolver(m_asio);
            boost::asio::ip::tcp::resolver::iterator it, end;

            it = resolver.resolve(boost::asio::ip::tcp::resolver::query(
                endpoint.first,
                boost::lexical_cast<std::string>(endpoint.second)
            ));

            m_resolver.reset(new api::resolve_t(
                m_context.log("resolver/" + m_name),
                m_asio,
                std::vector<boost::asio::ip::tcp::endpoint>(it, end)
            ));

            m_resolver->resolve(
                *m_client,
                m_name,
                m_cancellation.wrap(std::bind(&disposable_client::on_client_connected, this, std::placeholders::_1))
            );
        }
    }

    void
    on_client_connected(const boost::system::error_code& ec) {
        m_resolver.reset();

        if(!ec) {
            COCAINE_LOG_DEBUG(m_logger, "client connected");
            m_request_sender();
        } else {
            COCAINE_LOG_DEBUG(m_logger, "connection error: [%d] %s", ec.value(), ec.message())
            (blackhole::attribute::list({
                {"error_code", ec.value()},
                {"error_message", ec.message()}
            }));

            reset();
            try_next_remote();
        }
    }

    void
    on_timeout(const boost::system::error_code& ec) {
        if(!ec) {
            reset();
            try_next_remote();
        }
    }

    void
    resend_request(const boost::system::error_code& ec) {
        if(!ec) {
            reset_request();
            m_request_sender();
        }
    }

private:
    context_t &m_context;

    boost::asio::io_service& m_asio;

    cancel_t m_cancellation;

    const std::unique_ptr<logging::log_t> m_logger;

    // This timer resets connection and tries the next remote
    // if response from current remote was not received in m_request_timeout seconds.
    boost::asio::deadline_timer m_timeout_timer;

    // This timer resends request to current remote after m_request_timeout seconds
    // if the remote was busy.
    boost::asio::deadline_timer m_retry_timer;

    // Name of service.
    std::string m_name;

    // How many times the client should try a leader received from the current remote.
    unsigned int m_follow_redirect;

    unsigned int m_request_timeout;

    // Remote nodes, which will be used to find a leader.
    std::vector<node_id_t> m_remotes;

    // Next remote to use.
    size_t m_next_remote;

    // How many times the client already followed to leader received from remote.
    // When it becomes equal to m_follow_redirect, the client resets it it zero and tries the next
    // node from m_remotes.
    unsigned int m_redirection_counter;

    std::function<void()> m_error_handler;

    // Current operation. It's stored until it becomes committed to replicated state machine.
    std::function<void()> m_request_sender;

    std::unique_ptr<api::client<Tag>> m_client;

    std::unique_ptr<api::resolve_t> m_resolver;

    io::upstream_ptr_t m_current_request;
};

}} // namespace cocaine::raft

#endif // COCAINE_RAFT_CLIENT_HPP
