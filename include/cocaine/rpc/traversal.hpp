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

#ifndef COCAINE_IO_PROTOCOL_TRAVERSAL_HPP
#define COCAINE_IO_PROTOCOL_TRAVERSAL_HPP

#include "cocaine/rpc/graph.hpp"
#include "cocaine/rpc/protocol.hpp"

#include <boost/mpl/begin.hpp>
#include <boost/mpl/deref.hpp>
#include <boost/mpl/end.hpp>
#include <boost/mpl/next.hpp>

namespace cocaine { namespace io {

template<class Tag>
auto
traverse() -> boost::optional<dispatch_graph_t>;

namespace mpl = boost::mpl;

namespace aux {

template<class It, class End>
struct traverse_impl {
    typedef typename mpl::deref<It>::type message_type;
    typedef event_traits<message_type> traits_type;

    static inline
    void
    apply(dispatch_graph_t& object) {
        object[traits_type::id] = std::make_tuple(message_type::alias(),
            std::is_same<typename traits_type::transition_type, typename message_type::tag>::value
              ? boost::none
              : traverse<typename traits_type::transition_type>()
        );

        traverse_impl<typename mpl::next<It>::type, End>::apply(object);
    }
};

template<class End>
struct traverse_impl<End, End> {
    static inline
    void
    apply(dispatch_graph_t& /* object */) {
        // Empty.
    }
};

} // namespace aux

template<class Tag>
inline
auto
traverse() -> boost::optional<dispatch_graph_t> {
    dispatch_graph_t result;

    aux::traverse_impl<
        typename mpl::begin<typename protocol<Tag>::messages>::type,
        typename mpl::end<typename protocol<Tag>::messages>::type
    >::apply(result);

    return result;
}

}} // namespace cocaine::io

#endif
