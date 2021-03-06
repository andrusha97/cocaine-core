/*
    Copyright (c) 2011-2014 Andrey Sibiryov <me@kobology.ru>
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

#include "cocaine/detail/service/node/session.hpp"

#include "cocaine/rpc/queue.hpp"

#include "cocaine/traits/enum.hpp"
#include "cocaine/traits/frozen.hpp"
#include "cocaine/traits/literal.hpp"
#include "cocaine/traits/tuple.hpp"

using namespace cocaine::engine;
using namespace cocaine::io;

// Temporary adapter to join together `writable_stream` and `io::message_queue`.
// Guaranteed to live longer than the parent session.
class session_t::stream_adapter_t:
    public std::enable_shared_from_this<stream_adapter_t>
{
    session_t* m_session;
    std::shared_ptr<writable_stream<protocol_type, encoder_t>> m_downstream;

public:
    stream_adapter_t() = default;
    stream_adapter_t(session_t* session,
                     std::shared_ptr<writable_stream<protocol_type, encoder_t>> downstream):
        m_session(session),
        m_downstream(downstream)
    {}

    template<class Event, typename... Args>
    void
    send(Args&&... args) {
        m_downstream->write(
            io::encoded<Event>(m_session->id, std::forward<Args>(args)...),
            std::bind(&stream_adapter_t::on_write, shared_from_this(), ph::_1)
        );
    }

private:
    void
    on_write(const boost::system::error_code& ec) {
        if(ec) {
            if(ec == boost::asio::error::operation_aborted) {
                return;
            }

            m_session->close();
        }
    }
};

session_t::session_t(uint64_t id_, const api::event_t& event_, const api::stream_ptr_t& upstream_):
    id(id_),
    event(event_),
    upstream(upstream_),
    m_writer(new message_queue<io::rpc_tag, stream_adapter_t>),
    m_state(state::open)
{
    // Cache the invocation command right away.
    send<rpc::invoke>(event.name);
}

void
session_t::attach(const std::shared_ptr<writable_stream<protocol_type, encoder_t>>& downstream) {
    m_writer->attach(std::make_shared<stream_adapter_t>(this, downstream));
}

void
session_t::detach() {
    close();

    // Disable the session.
    m_writer.reset();
}

void
session_t::close() {
    std::lock_guard<std::mutex> lock(m_mutex);

    if(m_state == state::open) {
        send<rpc::choke>(lock);
        // There shouldn't be any other chunks after that.
        m_state = state::closed;
    }
}

session_t::downstream_t::downstream_t(const std::shared_ptr<session_t>& parent_):
    parent(parent_)
{ }

session_t::downstream_t::~downstream_t() {
    close();
}

void
session_t::downstream_t::write(const char* chunk, size_t size) {
    parent->send<rpc::chunk>(std::string(chunk, size));
}

void
session_t::downstream_t::error(int code, const std::string& reason) {
    parent->send<rpc::error>(code, reason);
}

void
session_t::downstream_t::close() {
    parent->close();
}
