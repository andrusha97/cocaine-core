/*
    Copyright (c) 2013-2014 Andrey Goryachev <andrey.goryachev@gmail.com>
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

#ifndef COCAINE_RAFT_REMOTE_HPP
#define COCAINE_RAFT_REMOTE_HPP

#include "cocaine/api/connect.hpp"
#include "cocaine/api/resolve.hpp"
#include "cocaine/idl/raft.hpp"
#include "cocaine/traits/graph.hpp"
#include "cocaine/traits/literal.hpp"

#include "cocaine/logging.hpp"

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>

namespace cocaine { namespace raft {

// This class holds communication with remote Raft node. It replicates entries and provides method
// to request vote.
template<class Cluster>
class remote_node {
    COCAINE_DECLARE_NONCOPYABLE(remote_node)

    typedef Cluster cluster_type;
    typedef typename cluster_type::actor_type actor_type;
    typedef typename actor_type::entry_type entry_type;
    typedef typename actor_type::snapshot_type snapshot_type;
    typedef io::raft_node<entry_type, snapshot_type> protocol;
    typedef io::raft_node_tag<entry_type, snapshot_type> protocol_tag;

    // This class handles response from remote node on append request.
    class vote_handler_t :
        public dispatch<io::stream_of<uint64_t, bool>::tag>
    {
        typedef io::protocol<io::stream_of<uint64_t, bool>::tag>::scope protocol;

    public:
        vote_handler_t(remote_node &remote):
            dispatch<io::stream_of<uint64_t, bool>::tag>(""),
            m_cancelled(false),
            m_remote(remote)
        {
            on<typename protocol::chunk>(
                std::bind(&vote_handler_t::on_write, this, std::placeholders::_1, std::placeholders::_2)
            );
            on<typename protocol::error>(std::bind(&vote_handler_t::on_error, this));
            on<typename protocol::choke>(std::bind(&vote_handler_t::on_error, this));
        }

        ~vote_handler_t() {
            on_error();
        }

        void
        on_write(uint64_t term, bool success) {
            // If the request is outdated, do nothing.
            if(cancelled()) {
                return;
            } else {
                cancel();
            }

            // Parent node stores pointer to handler of current uncompleted request.
            // We should reset this pointer, when the request becomes completed.
            m_remote.reset_vote_state();

            if(term > m_remote.m_actor.config().current_term()) {
                // Step down to the follower state if we live in an old term.
                m_remote.m_actor.step_down(term);
            } else if(success) {
                m_remote.m_won_term = m_remote.m_actor.config().current_term();
                m_remote.m_cluster.register_vote();
            }
        }

        void
        on_error() {
            if(!cancelled()) {
                cancel();
                m_remote.reset_vote_state();
            }
        }

        virtual
        void
        discard(const boost::system::error_code& COCAINE_UNUSED_(ec)) const {
            const_cast<vote_handler_t*>(this)->on_error();
        }

        // If the remote node doesn't need result of this request, it makes the handler inactive.
        // TODO: rewrite this logic to use upstream_t::revoke method.
        void
        cancel() {
            m_cancelled = true;
        }

        bool
        cancelled() const {
            return m_cancelled;
        }

    private:
        bool m_cancelled;
        remote_node &m_remote;
    };

    // This class handles response from remote node on append request.
    class append_handler_t :
        public dispatch<io::stream_of<uint64_t, bool>::tag>
    {
        typedef io::protocol<io::stream_of<uint64_t, bool>::tag>::scope protocol;

    public:
        append_handler_t(remote_node &remote):
            dispatch<io::stream_of<uint64_t, bool>::tag>(""),
            m_cancelled(false),
            m_remote(remote),
            m_last_index(0)
        {
            on<typename protocol::chunk>(
                std::bind(&append_handler_t::on_write, this, std::placeholders::_1, std::placeholders::_2)
            );
            on<typename protocol::error>(std::bind(&append_handler_t::on_error, this));
            on<typename protocol::choke>(std::bind(&append_handler_t::on_error, this));
        }

        ~append_handler_t() {
            on_error();
        }

        // Set index of last entry replicated with this request.
        void
        set_last(uint64_t last_index) {
            m_last_index = last_index;
        }

        void
        on_write(uint64_t term, bool success) {
            // If the request is outdated, do nothing.
            if(cancelled()) {
                return;
            } else {
                cancel();
            }

            // Parent node stores pointer to handler of current uncompleted append request.
            // We should reset this pointer, when the request becomes completed.
            m_remote.reset_append_state();

            if(term > m_remote.m_actor.config().current_term()) {
                // Step down to the follower state if we live in an old term.
                m_remote.m_actor.step_down(term);
                return;
            } else if(success) {
                // Mark the entries replicated and update commit index, if remote node returned success.
                m_remote.m_next_index = std::max(m_last_index + 1, m_remote.m_next_index);
                if(m_remote.m_match_index < m_last_index) {
                    m_remote.m_match_index = m_last_index;
                    m_remote.m_cluster.update_commit_index();
                }

                /* COCAINE_LOG_DEBUG(
                    m_remote.m_logger,
                    "Append request has been accepted. "
                    "New match index: %d, next entry to replicate: %d.",
                    m_remote.m_match_index,
                    m_remote.m_next_index
                ); */
            } else if(m_remote.m_next_index > 1) {
                // If follower discarded current request, try to replicate older entries.
                m_remote.m_next_index -= std::min<uint64_t>(
                    m_remote.m_actor.options().message_size,
                    m_remote.m_next_index - 1
                );

                /* COCAINE_LOG_DEBUG(
                    m_remote.m_logger,
                    "Append request has been discarded. "
                    "Match index: %d, next entry to replicate: %d.",
                    m_remote.m_match_index,
                    m_remote.m_next_index
                ); */
            } else {
                // The remote node discarded our oldest entries.
                // There is no sense to retry immediately.
                // Probably there is no sense to retry at all,
                // but what should the node do then but write something to log?
                COCAINE_LOG_WARNING(
                    m_remote.m_logger,
                    "follower %s:%d discarded our oldest entries, so it's impossible to replicate entries to this follower; "
                    "it's a wrong follower or there is an error in the RAFT implementation",
                    m_remote.id().first, m_remote.id().second
                );
                return;
            }

            // Continue replication. If there is no entries to replicate, this call does nothing.
            m_remote.replicate();
        }

        void
        on_error() {
            if(!cancelled()) {
                cancel();
                m_remote.reset_append_state();
            }
        }

        virtual
        void
        discard(const boost::system::error_code& COCAINE_UNUSED_(ec)) const {
            const_cast<append_handler_t*>(this)->on_error();
        }

        // If the remote node doesn't need result of this request, it makes the handler inactive.
        // TODO: rewrite this logic to use upstream_t::revoke method.
        void
        cancel() {
            m_cancelled = true;
        }

        bool
        cancelled() const {
            return m_cancelled;
        }

    private:
        bool m_cancelled;
        remote_node &m_remote;

        // Last entry replicated with this request.
        uint64_t m_last_index;
    };

public:
    remote_node(cluster_type& cluster, node_id_t id):
        m_cluster(cluster),
        m_actor(cluster.actor()),
        m_logger(m_actor.context().log(cocaine::format("raft/%s/remote/%d:%d", m_actor.name(), id.first, id.second))),
        m_id(id),
        m_heartbeat_timer(m_actor.asio()),
        m_next_index(std::max<uint64_t>(1, m_actor.log().last_index())),
        m_match_index(0),
        m_won_term(0),
        m_disconnected(false)
    { }

    ~remote_node() {
        finish_leadership();
    }

    const node_id_t&
    id() const {
        return m_id;
    }

    // Raft actor requests vote from the remote node via this method.
    void
    request_vote() {
        if(m_won_term >= m_actor.config().current_term()) {
            return;
        } else if(m_id == m_actor.context().raft().id()) {
            m_won_term = m_actor.config().current_term();
            m_cluster.register_vote();
        } else if(!m_vote_state) {
            m_vote_state = std::make_shared<vote_handler_t>(*this);
            // The default TCP keep-alive timeout is large and
            // may not provide an error for actually dead connection for a long time,
            // so let's start new term with new connection.
            m_client.reset();
            ensure_connection(std::bind(&remote_node::request_vote_impl, this));
        }
    }

    // When new entries are added to the log,
    // Raft actor tells this class that it's time to replicate.
    void
    replicate() {
        // TODO: Now leader sends one append request at the same time.
        // Probably it's possible to send requests in pipeline manner.
        // I should investigate this question.
        if(m_id == m_actor.context().raft().id()) {
            m_match_index = m_actor.log().last_index();
            m_cluster.update_commit_index();
        } else if(!m_append_state &&
                  m_actor.is_leader() &&
                  m_actor.log().last_index() >= m_next_index)
        {
            // Create new append state to mark, that there is active append request.
            m_append_state = std::make_shared<append_handler_t>(*this);
            ensure_connection(std::bind(&remote_node::replicate_impl, this));
        }
    }

    // Index of last entry replicated to the remote node.
    uint64_t
    match_index() const {
        return m_match_index;
    }

    // Index of last entry replicated to the remote node.
    uint64_t
    won_term() const {
        return m_won_term;
    }

    // Begin leadership. Actually it starts to send heartbeats.
    void
    begin_leadership() {
        if(m_id == m_actor.context().raft().id()) {
            m_match_index = m_actor.log().last_index();
            m_next_index = m_match_index + 1;
        } else {
            m_heartbeat_timer.expires_from_now(boost::posix_time::milliseconds(0));
            m_heartbeat_timer.async_wait(std::bind(&remote_node::heartbeat, this, std::placeholders::_1));
            // Now we don't know which entries are replicated to the remote.
            m_match_index = 0;
            m_next_index = std::max<uint64_t>(1, m_actor.log().last_index());
        }
    }

    // Stop sending heartbeats.
    void
    finish_leadership() {
        m_heartbeat_timer.cancel();
        reset();
    }

    bool
    disconnected() {
        return m_id != m_actor.context().raft().id() && m_disconnected;
    }

    // Reset current state of remote node.
    void
    reset() {
        // Close connection.
        m_resolver.reset();

        // Drop current requests.
        reset_vote_state();
        reset_append_state();

        // If connection error has occurred, then we don't know what entries are replicated.
        m_match_index = 0;
        m_next_index = std::max<uint64_t>(1, m_actor.log().last_index());
    }

private:
    // Drop current append request.
    void
    reset_append_state() {
        if(m_append_state) {
            m_append_state->cancel();
            m_append_state.reset();
        }
    }

    // Drop current vote request.
    void
    reset_vote_state() {
        if(m_vote_state) {
            m_vote_state->cancel();
            m_vote_state.reset();
        }
    }

    // Election stuff.

    void
    request_vote_impl() {
        if(m_client) {
            COCAINE_LOG_DEBUG(m_logger, "sending vote request");

            m_client->template invoke<typename protocol::request_vote>(
                m_vote_state,
                m_actor.name(),
                m_actor.config().current_term(),
                m_actor.context().raft().id(),
                std::make_tuple(m_actor.log().last_index(), m_actor.log().last_term())
            );
        } else {
            COCAINE_LOG_DEBUG(m_logger, "client isn't connected, unable to send vote request");
            reset_vote_state();
        }
    }

    // Leadership stuff.

    void
    replicate_impl() {
        if(!m_client || !m_actor.is_leader()) {
            COCAINE_LOG_DEBUG(m_logger,
                              "client isn't connected or the local node is not the leader, "
                              "unable to send append request");
            reset_append_state();
        } else if(m_next_index <= m_actor.log().snapshot_index()) {
            // If leader is far behind the leader, send snapshot.
            send_apply();
        } else if(m_next_index <= m_actor.log().last_index()) {
            // If there are some entries to replicate, then send them to the follower.
            send_append();
        }
    }

    void
    send_apply() {
        auto snapshot_entry = std::make_tuple(
            m_actor.log().snapshot_index(),
            m_actor.log().snapshot_term()
        );

        m_append_state->set_last(m_actor.log().snapshot_index());

        m_client->template invoke<typename protocol::apply>(
            m_append_state,
            m_actor.name(),
            m_actor.config().current_term(),
            m_actor.context().raft().id(),
            snapshot_entry,
            m_actor.log().snapshot(),
            m_actor.config().commit_index()
        );

        COCAINE_LOG_DEBUG(m_logger, "sending apply request")
        (blackhole::attribute::list({
            {"current_term", m_actor.config().current_term()},
            {"next_index", m_next_index},
            {"snapshot_index", m_actor.log().snapshot_index()}
        }));
    }

    void
    send_append() {
        // Term of prev_entry. Probably this logic could be in the log,
        // but I wanted to make log implementation as simple as possible,
        // because user can implement own log.
        // TODO: now m_actor.log() is a wrapper around user defined log, so I can move this logic there.
        uint64_t prev_term = (m_actor.log().snapshot_index() + 1 == m_next_index) ?
                             m_actor.log().snapshot_term() :
                             m_actor.log()[m_next_index - 1].term();

        // Index of last entry replicated with this request.
        uint64_t last_index = 0;

        // Send at most options().message_size entries in one message.
        if(m_next_index + m_actor.options().message_size <= m_actor.log().last_index()) {
            last_index = m_next_index + m_actor.options().message_size - 1;
        } else {
            last_index = m_actor.log().last_index();
        }

        // This vector stores entries to be sent.

        // TODO: We can send entries without copying.
        // We just need to implement type_traits to something like boost::Range.
        std::vector<entry_type> entries;

        for(uint64_t i = m_next_index; i <= last_index; ++i) {
            entries.push_back(m_actor.log()[i]);
        }

        m_append_state->set_last(last_index);

        m_client->template invoke<typename protocol::append>(
            m_append_state,
            m_actor.name(),
            m_actor.config().current_term(),
            m_actor.context().raft().id(),
            std::make_tuple(m_next_index - 1, prev_term),
            entries,
            m_actor.config().commit_index()
        );

        COCAINE_LOG_DEBUG(m_logger, "sending append request")
        (blackhole::attribute::list({
            {"current_term", m_actor.config().current_term()},
            {"next_index", m_next_index},
            {"last_index", m_actor.log().last_index()}
        }));
    }

    void
    send_heartbeat() {
        if(m_client) {
            COCAINE_LOG_DEBUG(m_logger, "sending heartbeat");

            std::tuple<uint64_t, uint64_t> prev_entry(0, 0);

            // Actually we don't need correct prev_entry to heartbeat,
            // but the follower will not accept commit index from request with old prev_entry.
            if(m_next_index - 1 <= m_actor.log().snapshot_index()) {
                prev_entry = std::make_tuple(m_actor.log().snapshot_index(),
                                             m_actor.log().snapshot_term());
            } else if(m_next_index - 1 <= m_actor.log().last_index()) {
                prev_entry = std::make_tuple(m_next_index - 1,
                                             m_actor.log()[m_next_index - 1].term());
            }

            m_client->template invoke<typename protocol::append>(
                nullptr,
                m_actor.name(),
                m_actor.config().current_term(),
                m_actor.context().raft().id(),
                prev_entry,
                std::vector<entry_type>(),
                m_actor.config().commit_index()
            );
        }
    }

    void
    heartbeat(const boost::system::error_code& ec) {
        if(ec) {
            return;
        }

        if(m_append_state || m_next_index > m_actor.log().last_index()) {
            // If there is nothing to replicate or there is active append request,
            // just send heartbeat.
            ensure_connection(std::bind(&remote_node::send_heartbeat, this));
        } else {
            replicate();
        }

        m_heartbeat_timer.expires_from_now(boost::posix_time::milliseconds(m_actor.options().heartbeat_timeout));
        m_heartbeat_timer.async_wait(std::bind(&remote_node::heartbeat, this, std::placeholders::_1));
    }

    // Connection maintenance.

    // Creates connection to remote Raft service and calls continuation (handler).
    void
    ensure_connection(const std::function<void()>& handler) {
        if(m_client && !m_resolver) {
            // Connection already exists.
            handler();
            // If m_resolver is not null then we're already connecting to the remote node,
            // and there is a pending operation. So just do nothing.
        } else if(!m_resolver) {
            COCAINE_LOG_DEBUG(m_logger, "client is not connected, connecting...");

            m_client.reset(new api::client<protocol_tag>);

            boost::asio::ip::tcp::resolver resolver(m_actor.asio());
            boost::asio::ip::tcp::resolver::iterator it, end;

            it = resolver.resolve(boost::asio::ip::tcp::resolver::query(
                m_id.first,
                boost::lexical_cast<std::string>(m_id.second)
            ));

            m_resolver.reset(new api::resolve_t(
                m_actor.context().log("resolve/" + m_actor.options().node_service_name),
                m_actor.asio(),
                std::vector<boost::asio::ip::tcp::endpoint>(it, end)
            ));

            m_resolver->resolve(
                *m_client,
                m_actor.options().node_service_name,
                std::bind(&remote_node::on_client_connected, this, handler, std::placeholders::_1)
            );
        }
    }

    void
    on_client_connected(const std::function<void()>& handler,
                        const boost::system::error_code& ec)
    {
        m_resolver.reset();

        if(ec) {
            COCAINE_LOG_DEBUG(m_logger,
                              "unable to connect to raft service: [%d] %s",
                              ec.value(),
                              ec.message())
            (blackhole::attribute::list({
                {"error_code", ec.value()},
                {"error_message", ec.message()}
            }));

            handler();
            reset();
        } else {
            m_disconnected = false;
            handler();
        }
    }

    void
    on_error(const std::error_code& ec) {
        COCAINE_LOG_DEBUG(m_logger, "connection error: [%d] %s", ec.value(), ec.message())
        (blackhole::attribute::list({
            {"error_code", ec.value()},
            {"error_message", ec.message()}
        }));

        reset();
        m_client.reset();
        m_disconnected = true;
        m_cluster.check_connections();
    }

private:
    cluster_type &m_cluster;

    actor_type &m_actor;

    const std::unique_ptr<logging::log_t> m_logger;

    // Id of the remote node.
    const node_id_t m_id;

    std::unique_ptr<api::client<protocol_tag>> m_client;

    std::unique_ptr<api::resolve_t> m_resolver;

    boost::asio::deadline_timer m_heartbeat_timer;

    // State of current append request.
    // When there is no active request, this pointer equals nullptr.
    std::shared_ptr<append_handler_t> m_append_state;

    std::shared_ptr<vote_handler_t> m_vote_state;

    // The next log entry to send to the follower.
    uint64_t m_next_index;

    // The last entry replicated to the follower.
    uint64_t m_match_index;

    // The last term, in which the node received vote from the remote.
    uint64_t m_won_term;

    // Indicates if the connection is lost (the connection can not be lost if it was not established).
    bool m_disconnected;
};

}} // namespace cocaine::raft

#endif // COCAINE_RAFT_REMOTE_HPP
