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

#ifndef COCAINE_IO_BUFFERED_WRITABLE_STREAM_HPP
#define COCAINE_IO_BUFFERED_WRITABLE_STREAM_HPP

#include "cocaine/asio/reactor.hpp"
#include "cocaine/asio/cancelable_task.hpp"

#include <cstring>

namespace cocaine { namespace io {

template<class Socket>
struct writable_stream {
    COCAINE_DECLARE_NONCOPYABLE(writable_stream)

    typedef Socket socket_type;
    typedef typename socket_type::endpoint_type endpoint_type;

    writable_stream(reactor_t& reactor, endpoint_type endpoint):
        m_socket(std::make_shared<socket_type>(endpoint)),
        m_socket_watcher(reactor.native()),
        m_reactor(reactor),
        m_tx_offset(0),
        m_wr_offset(0)
    {
        m_socket_watcher.set<writable_stream, &writable_stream::on_event>(this);
        m_ring.resize(65536);
    }

    writable_stream(reactor_t& reactor, const std::shared_ptr<socket_type>& socket):
        m_socket(socket),
        m_socket_watcher(reactor.native()),
        m_reactor(reactor),
        m_tx_offset(0),
        m_wr_offset(0)
    {
        m_socket_watcher.set<writable_stream, &writable_stream::on_event>(this);
        m_ring.resize(65536);
    }

    ~writable_stream() {
        unbind();
    }

    template<class ErrorHandler>
    void
    bind(ErrorHandler error_handler) {
        typedef std::function<void(const std::error_code&)> error_handler_type;
        m_handle_error = std::make_shared<error_handler_type>(error_handler);
    }

    void
    unbind() {
        if(m_socket_watcher.is_active()) {
            m_socket_watcher.stop();
        }

        m_handle_error.reset();
    }

    size_t
    footprint() const {
        return m_ring.size();
    }

    struct deferred_wakeup_action {
        void
        operator()() const { }
    };

    void
    write(const char* data, size_t size) {
        std::unique_lock<std::mutex> m_lock(m_ring_mutex);

        if(m_tx_offset == m_wr_offset) {
            std::error_code ec;

            // Nothing is pending in the ring so try to write directly to the socket, and enqueue
            // only the remaining part, if any. Ignore any errors here.
            ssize_t sent = m_socket->write(data, size, ec);

            if(sent > 0) {
                if(static_cast<size_t>(sent) == size) {
                    return;
                }

                data += sent;
                size -= sent;
            }
        }

        while(m_ring.size() - m_wr_offset < size) {
            size_t pending = m_wr_offset - m_tx_offset;

            if(pending + size > m_ring.size() / 2) {
                m_ring.resize(m_ring.size() * 2);
                continue;
            }

            // There's no space left at the end of the buffer, so copy all the unsent
            // data to the beginning and continue filling it from there.
            std::memmove(m_ring.data(), m_ring.data() + m_tx_offset, pending);

            m_wr_offset = pending;
            m_tx_offset = 0;
        }

        std::memcpy(m_ring.data() + m_wr_offset, data, size);

        m_wr_offset += size;

        if(!m_socket_watcher.is_active()) {
            m_socket_watcher.start(m_socket->fd(), ev::WRITE);
            m_reactor.post(deferred_wakeup_action());
        }
    }

private:
    void
    on_event(ev::io& /* io */, int /* revents */) {
        std::error_code ec;
        std::unique_lock<std::mutex> lock(m_ring_mutex);

        ssize_t sent = m_socket->write(
            m_ring.data() + m_tx_offset,
            m_wr_offset - m_tx_offset,
            ec
        );

        if(ec) {
            m_reactor.post(std::bind(make_task(m_handle_error), ec));
            m_socket_watcher.stop();
            return;
        }

        if(sent > 0) {
            m_tx_offset += sent;

            if(m_tx_offset == m_wr_offset) {
                m_socket_watcher.stop();
            }
        }
    }

private:
    const std::shared_ptr<socket_type> m_socket;

    // Socket poll object.
    ev::io m_socket_watcher;

    // Needed for asynchronous watcher control.
    reactor_t& m_reactor;

    // Ring buffer.
    std::vector<char> m_ring;

    off_t m_tx_offset,
          m_wr_offset;

    std::mutex m_ring_mutex;

    // Write error handler.
    std::shared_ptr<std::function<
        void(const std::error_code&)
    >> m_handle_error;
};

}} // namespace cocaine::io

#endif
