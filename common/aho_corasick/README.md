# Aho-Corasick Implementation (C++)

This is a header only implementation of the aho-corasick pattern search algorithm invented by Alfred V. Aho and Margaret J. Corasick. It is a very efficient dictionary matching algorithm that can locate all search patterns against in input text simultaneously in O(n + m), with space complexity O(m) (where n is the length of the input text, and m is the combined length of the search patterns).

To compile the code, your compiler must minimally support the following features of the C++11 standard:
- Range-based for loops.
- std::unique_ptr.
- auto.

Additionally, the benchmark makes use of chrono.

## Usage

The following will create a narrow string trie (for wide string support use aho_corasick::wtrie), add a couple of patterns to the trie, and then search for them in the input text.

```cpp
aho_corasick::trie trie;
trie.insert("hers");
trie.insert("his");
trie.insert("she");
trie.insert("he");
auto result = trie.parse_text("ushers");
```

It is also possible to remove overlapping instances, although it should be noted that this won't lower the runtime complexity. The rules that govern conflict resolution are 1. longer matches are favoured over shorter matches, and 2. left-most matches are favoured over right-most matches.

```cpp
aho_corasick::trie trie;
trie.remove_overlaps();
trie.insert("hot");
trie.insert("hot chocolate");
auto result = trie.parse_text("hot chocolate");
```

Sometimes it is relevant to search an input text which features a mixed case, making it harder to find matches. In this instance, the trie can lowercase the input text to ease the matching process.

```cpp
aho_corasick::trie trie;
trie.case_insensitive();
trie.insert("casing");
auto result = trie.parse_text("CaSiNg");
```

For some use-cases it is necessary to process both matching and non-matching text. In this case, you can use trie::tokenise.

```cpp
aho_corasick::trie trie;
trie.remove_overlaps()
    .only_whole_words()
    .case_insensitive();
trie.insert("great question");
trie.insert("forty-two");
trie.insert("deep thought");
auto tokens = trie.tokenise("The Answer to the Great Question... Of Life, the Universe and Everything... Is... Forty-two, said Deep Thought, with infinite majesty and calm.");
std::stringstream html;
html << "<html><body><p>";
for (const auto& token : tokens) {
	if (token.is_match()) html << "<i>";
	html << token.get_fragment();
	if (token.is_match()) html << "</i>";
}
html << "</p></body></html>";
std::cout << html.str();
```

## License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Further Reading

- [Wikipedia](https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_string_matching_algorithm)
- [Whitepaper](ftp://163.13.200.222/assistant/bearhero/prog/%A8%E4%A5%A6/ac_bm.pdf)