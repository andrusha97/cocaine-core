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

#ifndef COCAINE_IO_UPSTREAM_HPP
#define COCAINE_IO_UPSTREAM_HPP

#include "cocaine/asio/socket.hpp"
#include "cocaine/asio/tcp.hpp"

#include "cocaine/rpc/session.hpp"
#include "cocaine/rpc/channel.hpp"

namespace cocaine {

class upstream_t {
    const std::shared_ptr<session_t> session;
    const uint64_t index;

    bool automatically_revoke;

    struct states {
        enum values: int { active, sealed };
    };

    // NOTE: Sealed upstreams ignore any messages. At some point it might change to an explicit way
    // to show that the operation won't be completed.
    states::values state;

public:
    upstream_t(const std::shared_ptr<session_t>& session_, uint64_t index_):
        session(session_),
        index(index_),
        automatically_revoke(false),
        state(states::active)
    { }

    template<class Event, typename... Args>
    void
    send(Args&&... args);

    void
    auto_revoke(bool value = true) {
        automatically_revoke = value;
    }

    void
    revoke() {
        session->revoke(index);
    }
};

template<class Event, typename... Args>
void
upstream_t::send(Args&&... args) {
    if(state != states::active) {
        return;
    }

    if(std::is_same<typename io::event_traits<Event>::transition_type, void>::value) {
        state = states::sealed;

        // If the message transition type is void, i.e. the remote dispatch will be destroyed after
        // receiving this message, then revoke the channel with the given index in this session, so
        // that new requests might reuse it in the future. This upstream will become sealed.
        if(automatically_revoke) {
            revoke();
        }
    }

    std::lock_guard<std::mutex> guard(session->mutex);

    if(session->ptr) {
        session->ptr->wr->write<Event>(index, std::forward<Args>(args)...);
    }
}

} // namespace cocaine

#endif
