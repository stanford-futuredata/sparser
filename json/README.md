## Helpers for JSON data.

This directory contains the API for parsing JSON data with Sparser. Currently, we use RapidJSON as our downstream parser.

The APIs themselves are defined in the header files; the only build step
required is for RapidJSON itself. To do this,, `cd` into `rapidjson` and
follow the instructions in the ReadMe to install RapidJSON system-wide on
your machine.
