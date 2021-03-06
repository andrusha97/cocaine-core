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

#ifndef COCAINE_IO_RESULT_OF_HPP
#define COCAINE_IO_RESULT_OF_HPP

#include "cocaine/rpc/protocol.hpp"

#include "cocaine/tuple.hpp"

#include <boost/mpl/front.hpp>
#include <boost/mpl/size.hpp>

namespace cocaine { namespace io {

namespace mpl = boost::mpl;

struct mute_slot_tag;

namespace aux {

template<class T>
struct result_of_impl;

template<class T>
struct result_of_impl<streaming_tag<T>> {
    template<class U, size_t = mpl::size<U>::value>
    struct stream_element_type {
        typedef typename tuple::fold<U>::type type;
    };

    template<class U>
    struct stream_element_type<U, 1> {
        typedef typename mpl::front<U>::type type;
    };

    // In case there's only one type in the typelist, leave it as it is. Otherwise form a tuple out
    // of all the types in the typelist.
    typedef typename stream_element_type<T>::type type;
};

template<>
struct result_of_impl<streaming_tag<void>> {
    typedef void type;
};

template<>
struct result_of_impl<void> {
    // No messages will follow an invocation of this slot.
    typedef mute_slot_tag type;
};

}} // namespace io::aux

template<class Event>
struct result_of<Event, typename depend<typename Event::tag>::type> {
    typedef typename io::aux::result_of_impl<
        typename io::event_traits<Event>::upstream_type
    >::type type;
};

} // namespace cocaine

#endif
