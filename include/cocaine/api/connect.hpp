/*
    Copyright (c) 2013-2014 Andrey Goryachev <andrey.goryachev@gmail.com>
    Copyright (c) 2014 Andrey Sibiryov <me@kobology.ru>
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

#ifndef COCAINE_CONNECT_HPP
#define COCAINE_CONNECT_HPP

#include "cocaine/common.hpp"
#include "cocaine/locked_ptr.hpp"

#include "cocaine/rpc/dispatch.hpp"
#include "cocaine/rpc/session.hpp"
#include "cocaine/rpc/upstream.hpp"

namespace cocaine { namespace api {

template<class Tag> class client;

namespace details {

class basic_client_t {
    std::shared_ptr<session_t> m_session;

public:
    template<typename> friend class api::client;

    void
    connect(std::unique_ptr<boost::asio::ip::tcp::socket> socket);

    bool
    is_connected() const;

    auto
    session() const -> const session_t&;

    virtual
    int
    version() const = 0;

    // User-defined copy constructors in order to reconnect session signals.

    basic_client_t() = default;
    basic_client_t(const basic_client_t& other);

    virtual
   ~basic_client_t();

    basic_client_t&
    operator=(const basic_client_t& rhs);

private:
    void
    on_interrupt(const boost::system::error_code&);
};

} // namespace details

template<class Tag>
class client:
    public details::basic_client_t
{
    template<class Event>
    struct traits {
        typedef upstream<typename io::event_traits<Event>::dispatch_type>       upstream_type;
        typedef dispatch<typename io::event_traits<Event>::upstream_type> const dispatch_type;
    };

public:
    template<class Event, typename... Args>
    typename traits<Event>::upstream_type
    invoke(const std::shared_ptr<typename traits<Event>::dispatch_type>& dispatch, Args&&... args) {
        if(!is_connected()) {
            throw cocaine::error_t("client is not connected");
        }

        auto ptr = m_session->inject(dispatch);

        // TODO: Locking?
        // Check for message protocol compatibility.
        upstream<Tag>(ptr).template send<Event>(std::forward<Args>(args)...);

        return ptr;
    }

    virtual
    int
    version() const {
        return io::protocol<Tag>::version::value;
    }
};

}} // namespace cocaine::api

#endif
