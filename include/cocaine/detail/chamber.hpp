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

#ifndef COCAINE_CHAMBER_HPP
#define COCAINE_CHAMBER_HPP

#include "cocaine/common.hpp"
#include "cocaine/locked_ptr.hpp"

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>

#include <boost/asio/io_service.hpp>
#include <boost/asio/deadline_timer.hpp>

#define BOOST_BIND_NO_PLACEHOLDERS
#include <boost/thread/thread.hpp>

namespace cocaine { namespace io {

namespace baf = boost::accumulators;

class chamber_t {
    class named_runnable_t;
    class stats_periodic_action_t;

    static const unsigned int kCollectionInterval = 2;

    const std::string name;
    const std::shared_ptr<boost::asio::io_service> asio;

    // Takes resource usage snapshots every kCollectInterval seconds.
    std::unique_ptr<boost::asio::deadline_timer> cron;

    // This thread will run the reactor's event loop until terminated.
    std::unique_ptr<boost::thread> thread;

    typedef baf::accumulator_set<
        double,
        baf::features<baf::tag::rolling_mean>
    > load_average_t;

    // Rolling resource usage mean over last minute.
    synchronized<load_average_t> load_average;

public:
    chamber_t(const std::string& name, const std::shared_ptr<boost::asio::io_service>& asio);
   ~chamber_t();

    auto
    load_avg1() const -> double {
        return baf::rolling_mean(*load_average.synchronize());
    }

    auto
    uuid() const -> boost::thread::id {
        return thread->get_id();
    }
};

}} // namespace cocaine::io

#endif
