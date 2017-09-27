#!/usr/bin/env python

from __future__ import print_function

length_to_bits = lambda x: x * 8
maskvalues = {
        1: "0xffffffff",
        2: "0x55555555",
        4: "0x11111111",
        }

class SparserSearchCodeGen(object):

    def __init__(self, tokenlengths):
        self.tokenlengths = tokenlengths
        self.tokens = len(self.tokenlengths)

        # ID Index
        self.idx = 0

        assert len(self.tokenlengths) > 0

    def next_id(self):
        x = "t{}".format(self.idx)
        self.idx += 1
        return x

    def generate(self):
        buf = ""
        buf += self.generate_header()
        buf += self.generate_search_query_vectors()
        buf += self.generate_loop_start()
        buf += self.generate_loop_check_first_mask()
        buf += self.generate_loop_remaining_checks()
        buf += self.generate_tail()
        buf = buf.strip()
        print(buf)

    def generate_header(self):

        assertions_template =  "assert(query->lens[{token}] >= {tokenlength});"
        assertions_str = ""
        for i in xrange(self.tokens):
            assertions_str += assertions_template.format(token=i,
                    tokenlength=self.tokenlengths[i])
            assertions_str += "\n"

        template = """
        sparser_stats_t *sparser_search{tokens}x{tokenlengths}(char *input, long length,
                                        sparser_query_t *query,
                                        sparser_callback_t callback) {{

          assert(query->count == {tokens});
          {assertions}

          sparser_stats_t stats;
          memset(&stats, 0, sizeof(stats));
        """

        tokenlengths_str = "".join([str(x) + "x" for x in self.tokenlengths])
        tokenlengths_str = tokenlengths_str[:-1]

        output = template.format(tokens=self.tokens,
                tokenlengths=tokenlengths_str,
                assertions=assertions_str)
        return output


    def generate_search_query_vectors(self):
        template = """
        uint{bitlength}_t {id} = *((uint{bitlength}_t *)query->queries[{token}]);
        __m256i q{token} = _mm256_set1_epi{bitlength}({id});
        """
        output = ""
        for i in xrange(self.tokens):
            output += template.format(id=self.next_id(), bitlength=length_to_bits(self.tokenlengths[i]), token=i)
        return output

    def generate_loop_start(self):
        template = """
        // Bitmask designating which filters matched.
        // Bit i is set if if the ith filter matched for the current record.
        unsigned matchmask = 0;

        char *endptr = strchr(input, '\\n');
        long end;
        if (endptr) {{
        end = endptr - input;
        }} else {{
        end = length;
        }}

        for (long i = 0; i < length; i += VECSZ) {{
        if (i > end) {{
          char *endptr = strchr(input + i, '\\n');
          if (endptr) {{
            end = endptr - input;
          }} else {{
            end = length;
          }}
          matchmask = 0;
        }}
        """
        return template.format()

    def generate_loop_check_first_mask(self):

        # TODO shifts is not right...

        is_set_template = "!IS_SET(matchmask, {token})"
        ifcheck = "||".join([is_set_template.format(token=i) for i in xrange(self.tokens)])


        load_str_template = "__m256i val{offset} = _mm256_loadu_si256((__m256i const *)(base + {offset}));\n"

        mask_template_first = "unsigned mask = _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val2, q0));\n"
        mask_template = "mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val2, q0));\n"

        load = ""
        for i in xrange(max(self.tokenlengths)):
            load += load_str_template.format(offset=i)

        checkfirst = ""
        checkfirst += mask_template_first.format(bitlength=length_to_bits(self.tokenlengths[0]))
        for i in xrange(1, self.tokenlengths[0]):
            checkfirst += mask_template.format(bitlength=length_to_bits(self.tokenlengths[0]))


        template = """
        if ({ifcheck}) {{
            const char *base = input + i;
            {load}

            {checkfirst}

            mask &= {maskvalue};

            unsigned matched = _mm_popcnt_u32(mask);
            if (matched > 0) {{
            stats.total_matches += matched;
            matchmask |= 0x1;
            }}
            """
        return template.format(ifcheck=ifcheck,
                load=load,
                checkfirst=checkfirst,
                bitlength=length_to_bits(self.tokenlengths[0]),
                maskvalue=maskvalues[self.tokenlengths[0]])

        def generate_loop_remaining_checks(self):
            if len(self.tokens) == 1:
                return "}"

        first_check = "mask = _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val, q{token}));"
        first_check = first_check.format(token=self.tokens[1])

        template = """
          if (!IS_SET(matchmask, {token})) {{
            mask = _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val, q{token}));
            mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val2, q{token}));
            mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val3, q{token}));
            mask |= _mm256_movemask_epi8(_mm256_cmpeq_epi{bitlength}(val4, q{token}));
            mask &= {maskvalue};

            matched = _mm_popcnt_u32(mask);
            if (matched > 0) {{
              stats.total_matches += matched;
              matchmask |= (0x1 << 1);
            }}
        """

        output = ""
        for token in xrange(1, self.tokens):
            output += template.format(token=token,
                    maskvalue=maskvalues[self.tokenlengths[token]],
                    bitlength=length_to_bits(self.tokenlengths[token]))
            for _ in xrange(self.tokens):
                output += "}\n"
        return output

    def generate_tail(self):
        return """
            unsigned allset = ((1u << query->count) - 1u);
            // check if all the filters matched by checking if all the bits
            // necessary were set in matchmask.
            if ((matchmask & allset) == allset) {{
              stats.sparser_passed++;

              // update start.
              long start = i;
              for (; start > 0 && input[start] != '\\n'; start--)
                ;

              stats.bytes_seeked_backward += (i - start);

              // Pass the current line to a full parser.
              char a = input[end];
              input[end] = '\\0';
              if (callback(input + start)) {{
                stats.callback_passed++;
              }}
              input[end] = a;

              // Reset record level state.
              matchmask = 0;

              // Done with this record - move on to the next one.
              i = end + 1 - VECSZ;
            }}
          }}

          if (stats.sparser_passed > 0) {{
            stats.fraction_passed_correct =
                (double)stats.callback_passed / (double)stats.sparser_passed;
            stats.fraction_passed_incorrect = 1.0 - stats.fraction_passed_correct;
          }}

          sparser_stats_t *ret = (sparser_stats_t *)malloc(sizeof(sparser_stats_t));
          memcpy(ret, &stats, sizeof(stats));

          return ret;
          }}
      """.format()

import sys

def usage(name):
    print("{} <tokenlengths>".format(name))

if __name__ == "__main__":

    if len(sys.argv) != 2:
        usage(sys.argv[0])
        sys.exit(1)

    lens = [int(x) for x in sys.argv[1].strip().split(",")]
    x = SparserSearchCodeGen(lens)
    x.generate()
