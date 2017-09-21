/*
 * Copyright (C) 2015 Christopher Gilbert.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define CATCH_CONFIG_MAIN
#include "../test/catch.hpp"

#include "aho_corasick/aho_corasick.hpp"
#include <set>

namespace ac = aho_corasick;

TEST_CASE("intervals work as required", "[interval]") {
	SECTION("construct") {
		ac::interval i(1, 3);
		REQUIRE(1 == i.get_start());
		REQUIRE(3 == i.get_end());
	}
	SECTION("size") {
		REQUIRE(3 == ac::interval(0, 2).size());
	}
	SECTION("interval overlaps") {
		REQUIRE(ac::interval(1, 3).overlaps_with(ac::interval(2, 4)));
	}
	SECTION("interval does not overlap") {
		REQUIRE(!ac::interval(1, 13).overlaps_with(ac::interval(27, 42)));
	}
	SECTION("point overlaps") {
		REQUIRE(ac::interval(1, 3).overlaps_with(2));
	}
	SECTION("point does not overlap") {
		REQUIRE(!ac::interval(1, 13).overlaps_with(42));
	}
	SECTION("comparable") {
		std::set<ac::interval> intervals;
		intervals.insert(ac::interval(4, 6));
		intervals.insert(ac::interval(2, 7));
		intervals.insert(ac::interval(3, 4));
		auto it = intervals.begin();
		REQUIRE(2 == it++->get_start());
		REQUIRE(3 == it++->get_start());
		REQUIRE(4 == it++->get_start());
	}
}