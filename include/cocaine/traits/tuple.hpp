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

#ifndef COCAINE_TYPELIST_SERIALIZATION_TRAITS_HPP
#define COCAINE_TYPELIST_SERIALIZATION_TRAITS_HPP

#include "cocaine/common.hpp"
#include "cocaine/traits.hpp"

#include "cocaine/rpc/tags.hpp"

#include <tuple>

#include <boost/mpl/begin.hpp>
#include <boost/mpl/count_if.hpp>
#include <boost/mpl/deref.hpp>
#include <boost/mpl/is_sequence.hpp>
#include <boost/mpl/next.hpp>

namespace cocaine { namespace io {

// NOTE: The following structure is a template specialization for type lists, to support validating
// sequence packing and unpacking with optional elements, which can be used as follows:
//
// type_traits<Sequence>::pack(buffer, std::forward<Args>(args)...);
// type_traits<Sequence>::unpack(object, std::forward<Args>(args)...);
//
// Or with tuples:
//
// type_traits<Sequence>::pack(buffer, std::tuple<Args...> tuple);
// type_traits<Sequence>::unpack(object, std::tuple<Args...> tuple);

namespace aux {

template<class IndexSequence>
struct tuple_type_traits_impl;

template<size_t... Indices>
struct tuple_type_traits_impl<index_sequence<Indices...>> {
    template<class Sequence, class Stream, class Tuple>
    static inline
    void
    pack(msgpack::packer<Stream>& target, const Tuple& source) {
        type_traits<Sequence>::pack(target, std::get<Indices>(source)...);
    }

    template<class Sequence, class Tuple>
    static inline
    void
    unpack(const msgpack::object& source, Tuple& target) {
        type_traits<Sequence>::unpack(source, std::get<Indices>(target)...);
    }
};

template<class T>
struct unpack_sequence_impl {
    template<class SourceIterator>
    static inline
    SourceIterator
    apply(SourceIterator it, SourceIterator /* end */, T& target) {
        // The only place where the source iterator is actually could be incremented, all other
        // unpackers either delegate to this one, or don't touch the source at all.
        type_traits<T>::unpack(*it++, target);

        return it;
    }
};

template<class T>
struct unpack_sequence_impl<optional<T>> {
    template<class SourceIterator>
    static inline
    SourceIterator
    apply(SourceIterator it, SourceIterator end, T& target) {
        if(it != end) {
            return unpack_sequence_impl<T>::apply(it, end, target);
        } else {
            target = T();
        }

        return it;
    }
};

template<class T, T Default>
struct unpack_sequence_impl<optional_with_default<T, Default>> {
    template<class SourceIterator>
    static inline
    SourceIterator
    apply(SourceIterator it, SourceIterator end, T& target) {
        if(it != end) {
            return unpack_sequence_impl<T>::apply(it, end, target);
        } else {
            target = Default;
        }

        return it;
    }
};

} // namespace aux

// Variadic pack serialization

template<class T>
struct type_traits<
    T,
    typename std::enable_if<boost::mpl::is_sequence<T>::value>::type
>
{
    enum constants: unsigned {

    minimal = boost::mpl::count_if<
        T,
        boost::mpl::lambda<detail::is_required<boost::mpl::_1>>
    >::value

    };

public:
    template<class Stream, typename... Args>
    static inline
    void
    pack(msgpack::packer<Stream>& target, const Args&... sources) {
        static_assert(sizeof...(sources) >= minimal, "sequence length mismatch");

        // The sequence will be packed as an array.
        target.pack_array(sizeof...(sources));

        // Recursively pack every sequence element.
        pack_sequence<typename boost::mpl::begin<T>::type>(target, sources...);
    }

    template<class Stream, typename... Args>
    static inline
    void
    pack(msgpack::packer<Stream>& target, const std::tuple<Args...>& source) {
        typedef aux::tuple_type_traits_impl<
            typename make_index_sequence<sizeof...(Args)>::type
        > traits_type;

        traits_type::template pack<T>(target, source);
    }

    #if defined(__GNUC__) && !defined(HAVE_GCC46)
        #pragma GCC diagnostic ignored "-Wtype-limits"
    #endif

    template<typename... Args>
    static inline
    void
    unpack(const msgpack::object& source, Args&... targets) {
        static_assert(sizeof...(targets) >= minimal, "sequence length mismatch");

        // NOTE: In cases when the callable is nullary or every parameter is optional, the sequence
        // length comparison is tautological and yields dead code (unsigned integer < 0). This is a
        // known compiler bug: http://llvm.org/bugs/show_bug.cgi?id=8682

        #if defined(__GNUC__) && defined(HAVE_GCC46)
            #pragma GCC diagnostic push
            #pragma GCC diagnostic ignored "-Wtype-limits"
        #endif

        if(source.type != msgpack::type::ARRAY) {
            throw cocaine::error_t("sequence type mismatch");
        } else if(source.via.array.size < minimal) {
            throw cocaine::error_t("sequence length mismatch - expected at least %d, got %d", minimal, source.via.array.size);
        }

        #if defined(__GNUC__) && defined(HAVE_GCC46)
            #pragma GCC diagnostic pop
        #endif

        std::vector<msgpack::object> objects(
            source.via.array.ptr,
            source.via.array.ptr + source.via.array.size
        );

        // Recursively unpack every tuple element while validating the types.
        unpack_sequence<typename boost::mpl::begin<T>::type>(objects.begin(), objects.end(), targets...);
    }

    template<typename... Args>
    static inline
    void
    unpack(const msgpack::object& source, std::tuple<Args...>& target) {
        typedef aux::tuple_type_traits_impl<
            typename make_index_sequence<sizeof...(Args)>::type
        > traits_type;

        traits_type::template unpack<T>(source, target);
    }

private:
    template<class It, class Stream>
    static inline
    void
    pack_sequence(msgpack::packer<Stream>& /* target */) {
        // Empty.
    }

    template<class It, class Stream, class Head, typename... Tail>
    static inline
    void
    pack_sequence(msgpack::packer<Stream>& target, const Head& head, const Tail&... tail) {
        typedef typename pristine<Head>::type type;
        typedef typename boost::mpl::deref<It>::type element_type;

        static_assert(
            std::is_convertible<type, typename detail::unwrap_type<element_type>::type>::value,
            "sequence element type mismatch"
        );

        // Pack the current element using the correct packer.
        type_traits<type>::pack(target, head);

        // Recurse to the next element.
        pack_sequence<typename boost::mpl::next<It>::type>(target, tail...);
    }

    template<class It, class SourceIterator>
    static inline
    void
    unpack_sequence(SourceIterator /* it */, SourceIterator /* end */) {
        // Empty.
    }

    template<class It, class SourceIterator, class Head, typename... Tail>
    static inline
    void
    unpack_sequence(SourceIterator it, SourceIterator end, Head& head, Tail&... tail) {
        typedef typename pristine<Head>::type type;
        typedef typename boost::mpl::deref<It>::type element_type;

        static_assert(
            std::is_convertible<type, typename detail::unwrap_type<element_type>::type>::value,
            "sequence element type mismatch"
        );

        // Unpack the current element using the correct packer.
        it = aux::unpack_sequence_impl<element_type>::apply(it, end, head);

        // Recurse to the next element.
        unpack_sequence<typename boost::mpl::next<It>::type>(it, end, tail...);
    }
};

// Tuple serialization

template<typename... Args>
struct type_traits<std::tuple<Args...>> {
    typedef typename itemize<Args...>::type sequence_type;

    typedef aux::tuple_type_traits_impl<
        typename make_index_sequence<sizeof...(Args)>::type
    > traits_type;

    template<class Stream>
    static inline
    void
    pack(msgpack::packer<Stream>& target, const std::tuple<Args...>& source) {
        traits_type::template pack<sequence_type>(target, source);
    }

    static inline
    void
    unpack(const msgpack::object& source, std::tuple<Args...>& target) {
        traits_type::template unpack<sequence_type>(source, target);
    }
};

}} // namespace cocaine::io

namespace msgpack {

template<typename... Args>
inline
std::tuple<Args...>&
operator>>(object o, std::tuple<Args...>& t) {
    cocaine::io::type_traits<std::tuple<Args...>>::unpack(o, t);
    return t;
}

template<class Stream, typename... Args>
inline
packer<Stream>&
operator<<(packer<Stream>& o, const std::tuple<Args...>& t) {
    cocaine::io::type_traits<std::tuple<Args...>>::pack(o, t);
    return o;
}

} // namespace msgpack

#endif
