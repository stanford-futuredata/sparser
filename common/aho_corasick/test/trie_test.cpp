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
#include <string>

namespace ac = aho_corasick;

TEST_CASE("trie works as required", "[trie]") {
	auto check_emit = [](const ac::emit<char>& next, size_t expect_start, size_t expect_end, std::string expect_keyword) -> void {
		REQUIRE(expect_start == next.get_start());
		REQUIRE(expect_end == next.get_end());
		REQUIRE(expect_keyword == next.get_keyword());
	};
	auto check_wemit = [](const ac::emit<wchar_t>& next, size_t expect_start, size_t expect_end, std::wstring expect_keyword) -> void {
		REQUIRE(expect_start == next.get_start());
		REQUIRE(expect_end == next.get_end());
		REQUIRE(expect_keyword == next.get_keyword());
	};
	auto check_token = [](const ac::trie::token_type& next, std::string expect_fragment) -> void {
		REQUIRE(expect_fragment == next.get_fragment());
	};
	SECTION("keyword and text are the same") {
		ac::trie t;
		t.insert("abc");
		auto emits = t.parse_text("abc");
		auto it = emits.begin();
		check_emit(*it, 0, 2, "abc");
	}
	SECTION("text is longer than the keyword") {
		ac::trie t;
		t.insert("abc");

		auto emits = t.parse_text(" abc");

		auto it = emits.begin();
		check_emit(*it, 1, 3, "abc");
	}
	SECTION("various keywords one match") {
		ac::trie t;
		t.insert("abc");
		t.insert("bcd");
		t.insert("cde");

		auto emits = t.parse_text("bcd");

		auto it = emits.begin();
		check_emit(*it, 0, 2, "bcd");
	}
	SECTION("ushers test") {
		ac::trie t;
		t.insert("hers");
		t.insert("his");
		t.insert("she");
		t.insert("he");

		auto emits = t.parse_text("ushers");
		REQUIRE(3 == emits.size());

		auto it = emits.begin();
		check_emit(*it++, 2, 3, "he");
		check_emit(*it++, 1, 3, "she");
		check_emit(*it++, 2, 5, "hers");
	}
	SECTION("misleading test") {
		ac::trie t;
		t.insert("hers");

		auto emits = t.parse_text("h he her hers");

		auto it = emits.begin();
		check_emit(*it++, 9, 12, "hers");
	}
	SECTION("recipes") {
		ac::trie t;
		t.insert("veal");
		t.insert("cauliflower");
		t.insert("broccoli");
		t.insert("tomatoes");

		auto emits = t.parse_text("2 cauliflowers, 3 tomatoes, 4 slices of veal, 100g broccoli");
		REQUIRE(4 == emits.size());

		auto it = emits.begin();
		check_emit(*it++, 2, 12, "cauliflower");
		check_emit(*it++, 18, 25, "tomatoes");
		check_emit(*it++, 40, 43, "veal");
		check_emit(*it++, 51, 58, "broccoli");
	}
	SECTION("long and short overlapping match") {
		ac::trie t;
		t.insert("he");
		t.insert("hehehehe");

		auto emits = t.parse_text("hehehehehe");
		REQUIRE(7 == emits.size());

		auto it = emits.begin();
		check_emit(*it++, 0, 1, "he");
		check_emit(*it++, 2, 3, "he");
		check_emit(*it++, 4, 5, "he");
		check_emit(*it++, 6, 7, "he");
		check_emit(*it++, 0, 7, "hehehehe");
		check_emit(*it++, 8, 9, "he");
		check_emit(*it++, 2, 9, "hehehehe");
	}
	SECTION("non overlapping") {
		ac::trie t;
		t.remove_overlaps();
		t.insert("ab");
		t.insert("cba");
		t.insert("ababc");

		auto emits = t.parse_text("ababcbab");
		REQUIRE(2 == emits.size());

		auto it = emits.begin();
		check_emit(*it++, 0, 4, "ababc");
		check_emit(*it++, 6, 7, "ab");
	}
	SECTION("partial match") {
		ac::trie t;
		t.only_whole_words();
		t.insert("sugar");

		auto emits = t.parse_text("sugarcane sugarcane sugar canesugar");
		REQUIRE(1 == emits.size());

		auto it = emits.begin();
		check_emit(*it, 20, 24, "sugar");
	}
	SECTION("tokenise tokens in sequence") {
		ac::trie t;
		t.insert("Alpha");
		t.insert("Beta");
		t.insert("Gamma");

		auto tokens = t.tokenise("Alpha Beta Gamma");
		REQUIRE(5 == tokens.size());
	}
	SECTION("tokenise full sentence") {
		ac::trie t;
		t.only_whole_words();
		t.insert("Alpha");
		t.insert("Beta");
		t.insert("Gamma");

		auto tokens = t.tokenise("Hear: Alpha team first, Beta from the rear, Gamma in reserve");
		REQUIRE(7 == tokens.size());

		auto it = tokens.begin();
		check_token(*it++, "Hear: ");
		check_token(*it++, "Alpha");
		check_token(*it++, " team first, ");
		check_token(*it++, "Beta");
		check_token(*it++, " from the rear, ");
		check_token(*it++, "Gamma");
		check_token(*it++, " in reserve");
	}
	SECTION("wtrie case insensitive") {
		ac::wtrie t;
		t.case_insensitive().only_whole_words();
		t.insert(L"turning");
		t.insert(L"once");
		t.insert(L"again");

		auto emits = t.parse_text(L"TurninG OnCe AgAiN");
		REQUIRE(3 == emits.size());

		auto it = emits.begin();
		check_wemit(*it++, 0, 6, L"turning");
		check_wemit(*it++, 8, 11, L"once");
		check_wemit(*it++, 13, 17, L"again");
	}
	SECTION("trie case insensitive") {
		ac::trie t;
		t.case_insensitive();
		t.insert("turning");
		t.insert("once");
		t.insert("again");

		auto emits = t.parse_text("TurninG OnCe AgAiN");
		REQUIRE(3 == emits.size());

		auto it = emits.begin();
		check_emit(*it++, 0, 6, "turning");
		check_emit(*it++, 8, 11, "once");
		check_emit(*it++, 13, 17, "again");
	}
}