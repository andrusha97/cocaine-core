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

#ifndef COCAINE_LOGGING_SERVICE_HPP
#define COCAINE_LOGGING_SERVICE_HPP

#include "cocaine/api/service.hpp"

#include "cocaine/dispatch.hpp"
#include "cocaine/rpc/tags.hpp"

namespace cocaine { namespace service {

class logging_t:
    public api::service_t,
    public implementation<io::logging_tag>
{
    public:
        logging_t(context_t& context, io::reactor_t& reactor, const std::string& name, const dynamic_t& args);

        virtual dispatch_t& prototype() {
            return *this;
        }
};

}} // namespace cocaine::service

#endif
