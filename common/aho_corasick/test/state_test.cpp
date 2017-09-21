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

namespace ac = aho_corasick;

TEST_CASE("state works as required", "[state]") {
	SECTION("construct character sequence") {
		auto root = new ac::state<char>();
		root
			->add_state('a')
			->add_state('b')
			->add_state('c');
		auto cur_state = root->next_state('a');
		REQUIRE(1 == cur_state->get_depth());
		cur_state = cur_state->next_state('b');
		REQUIRE(2 == cur_state->get_depth());
		cur_state = cur_state->next_state('c');
		REQUIRE(3 == cur_state->get_depth());
		delete root;
	}
}