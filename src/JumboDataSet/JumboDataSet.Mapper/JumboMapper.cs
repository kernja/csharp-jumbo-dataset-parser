using System.Data;

namespace JumboDataSet.Mapper
{
    public class JumboMapper
    {
        private const string RESULTSET_COLUMN_NAME = "SET_COLUMN";
        private const string RESULTSET_COLUMN_DELIMITER = "_";

        public string ResultSetColumnName { get; }
        public string Delimiter { get; }

        public JumboMapper() : this(RESULTSET_COLUMN_NAME, RESULTSET_COLUMN_DELIMITER)
        {       
        }

        /// <summary>
        /// Parses a jumbo data table into a dataset with numerous individual tables.
        /// </summary>
        /// <param name="pResultSet">Dataset that consists of just one jumbo datatable.</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public DataSet Map(DataSet pResultSet)
        {
            ArgumentNullException.ThrowIfNull(pResultSet);
            if (pResultSet.Tables.Count != 1) throw new ArgumentOutOfRangeException(nameof(pResultSet.Tables));

            var jumboTable = pResultSet.Tables[0];
            var destResultSet = new DataSet();

            foreach (var id in Step1_GetDistinctResultSetIdentifiers(jumboTable))
            {
                var columnMappings = Step2_GetResultSetColumnMappings(jumboTable, id);
                var destTable = Step3_CreateEmptyDestinationTable(columnMappings);

                foreach (var row in Step4_GetResultSetRows(jumboTable, id))
                {
                    Step5_SetDestinationTableValues(ref destTable, row, columnMappings);
                }

                destResultSet.Tables.Add(destTable);
            }

            return destResultSet;
        }

        public JumboMapper(string pResultSetColumnName, string pDelimiter)
        {
            if (string.IsNullOrWhiteSpace(pResultSetColumnName)) throw new ArgumentNullException(nameof(pResultSetColumnName));
            if (string.IsNullOrWhiteSpace(pDelimiter)) throw new ArgumentNullException(nameof(pDelimiter));

            ResultSetColumnName = pResultSetColumnName;
            Delimiter = pDelimiter;
        }

        
        /// <summary>
        /// Returns all distinct table identifiers based off column names and delimiter within a jumbo table. 
        /// </summary>
        /// <param name="pTable">Jumbo datatable consisting of numerous individual tables.</param>
        public IList<string> Step1_GetDistinctResultSetIdentifiers(DataTable pTable)
        {
            ArgumentNullException.ThrowIfNull(pTable);

            var resultSetColumn = GetResultSetColumn(pTable);
            return pTable.Columns.Cast<DataColumn>().Where(x => x != resultSetColumn).Select(y => GetSuffixWithDelimiter(y.ColumnName)).Distinct().OrderBy(z => z).ToList();
        }

        /// <summary>
        /// Returns all source-to-destination column mappings for copying data from the jumbo table to an individual table. For example, [("BEE_02", "BEE"), ("COW_02", "COW")].
        /// </summary>
        /// <param name="pTable">Jumbo datatable consisting of numerous individual tables.</param>
        /// <param name="pSuffix">Value that identifies an individual table within a jumbo datatable.</param>
        public IList<(string source, string destination)> Step2_GetResultSetColumnMappings(DataTable pTable, string pSuffix)
        {
            ArgumentNullException.ThrowIfNull(pTable);
            ArgumentNullException.ThrowIfNullOrWhiteSpace(pSuffix);

            var applicableColumns = pTable.Columns.Cast<DataColumn>().Where(x => x.ColumnName.EndsWith(pSuffix)).Select(y => y.ColumnName).ToList();
            return applicableColumns.Select(x => (x, RemoveSuffix(x))).ToList();
        }

        /// <summary>
        /// Returns an individual datatable with column names set by mapping destination values.
        /// </summary>
        /// <param name="pColumnMappings">All source-to-destination column mappings for copying data from the jumbo table to an individual table. For example, [("BEE_02", "BEE"), ("COW_02", "COW")].</param>
        public DataTable Step3_CreateEmptyDestinationTable(IList<(string source, string destination)> pColumnMappings)
        {
            ArgumentNullException.ThrowIfNull(pColumnMappings);

            var destinationTable = new DataTable();
            foreach (var i in pColumnMappings)
            {
                destinationTable.Columns.Add(i.destination);
            }

            return destinationTable;
        }

        /// <summary>
        /// Returns all rows from the jumbo datatable that have a set-column value that ends with the passed-in suffix value.
        /// </summary>
        /// <param name="pTable">Jumbo datatable consisting of numerous individual tables.</param>
        /// <param name="pSuffix">Value that identifies an individual table within a jumbo datatable.</param>
        public IList<DataRow> Step4_GetResultSetRows(DataTable pTable, string pSuffix)
        {
            ArgumentNullException.ThrowIfNull(pTable);
            ArgumentException.ThrowIfNullOrWhiteSpace(pSuffix);
            
            var resultSetColumn = GetResultSetColumn(pTable);
            return pTable.AsEnumerable().Where(x => x[resultSetColumn].ToString().EndsWith(GetSuffix(pSuffix))).ToList();
        }


        /// <summary>
        /// Creates and adds a new row in the destination table using column mappings with values from the passed-in datarow.
        /// </summary>
        /// <param name="pDestinationTable">Individual table created from the jumbo datatable.</param>
        /// <param name="pRow">Single row from the jumbo datatable.</param>
        /// <param name="pColumnMappings">All source-to-destination column mappings for copying data from the jumbo table to an individual table. For example, [("BEE_02", "BEE"), ("COW_02", "COW")].</param>
        public void Step5_SetDestinationTableValues(ref DataTable pDestinationTable, DataRow pRow, IList<(string source, string destination)> pColumnMappings)
        {
            ArgumentNullException.ThrowIfNull(pRow);
            ArgumentNullException.ThrowIfNull(pColumnMappings);

            var newRow = pDestinationTable.NewRow();
            foreach(var i in pColumnMappings)
            {
                newRow[i.destination] = pRow[i.source];
            }

            pDestinationTable.Rows.Add(newRow);
        }

        /// <summary>
        /// Returns the column that ties rows to individual tables.
        /// </summary>
        /// <param name="pTable">Jumbo datatable consisting of numerous individual tables.</param>
        public DataColumn GetResultSetColumn(DataTable pTable)
        {
            ArgumentNullException.ThrowIfNull(pTable);

            DataColumn? result = pTable.Columns.Cast<DataColumn>().FirstOrDefault(x => string.Equals(x.ColumnName, ResultSetColumnName, StringComparison.InvariantCultureIgnoreCase));
            if (result == null) throw new Exception("Resultset column not found.");

            return result;
        }

        /// <summary>
        /// Removes the suffix (e.g., "ANT_01" -> "ANT") from a given string and returns the value.
        /// </summary>
        /// <param name="pString">String value with a suffix.</param>
        public string RemoveSuffix(string pString)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pString);

            var splitString = pString.Split(Delimiter).ToList();
            splitString.Reverse();
            splitString.RemoveAt(0);
            splitString.Reverse();
            return string.Join(Delimiter, splitString);
        }

        /// <summary>
        /// Returns delimiter and suffix (e.g., "ANT_01" -> "_01") from a given string and returns the value.
        /// </summary>
        /// <param name="pString">String value with a suffix.</param>
        public string GetSuffixWithDelimiter(string pString)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pString);

            var splitString = pString.Split(Delimiter).ToList();
            splitString.Reverse();
            return string.Format($"{Delimiter}{splitString[0]}");
        }

        /// <summary>
        /// Returns suffix (e.g., "ANT_01" -> "01") from a given string and returns the value.
        /// </summary>
        /// <param name="pString">String value with a suffix.</param>
        public string GetSuffix(string pString)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pString);

            var splitString = pString.Split(Delimiter).ToList();
            splitString.Reverse();
            return splitString[0];
        }

    }
}
