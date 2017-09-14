/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.stanford.sparser

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object sparser {
  /**
   * Adds a method, `sparser`, to DataFrameReader that allows you to read CSV/JSON files using
   * Sparser combined with the DataFileReader
   */
  implicit class SparserDataFrameReader(reader: DataFrameReader) {
    def sparser: String => DataFrame = reader.format("edu.stanford.sparser").load
  }
}
