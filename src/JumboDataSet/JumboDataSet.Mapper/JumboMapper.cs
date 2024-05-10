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


        public IList<string> Step1_GetDistinctResultSetIdentifiers(DataTable pTable)
        {
            ArgumentNullException.ThrowIfNull(pTable);

            var resultSetColumn = GetResultSetColumn(pTable);
            return pTable.Columns.Cast<DataColumn>().Where(x => x != resultSetColumn).Select(y => GetSuffixWithDelimiter(y.ColumnName)).Distinct().OrderBy(z => z).ToList();
        }

        public IList<(string source, string destination)> Step2_GetResultSetColumnMappings(DataTable pTable, string pSuffix)
        {
            ArgumentNullException.ThrowIfNull(pTable);
            ArgumentNullException.ThrowIfNullOrWhiteSpace(pSuffix);

            var applicableColumns = pTable.Columns.Cast<DataColumn>().Where(x => x.ColumnName.EndsWith(pSuffix)).Select(y => y.ColumnName).ToList();
            return applicableColumns.Select(x => (x, RemoveSuffix(x))).ToList();
        }

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

        public IList<DataRow> Step4_GetResultSetRows(DataTable pTable, string pResultSetIdentifier)
        {
            ArgumentNullException.ThrowIfNull(pTable);
            ArgumentException.ThrowIfNullOrWhiteSpace(pResultSetIdentifier);
            
            var resultSetColumn = GetResultSetColumn(pTable);
            return pTable.AsEnumerable().Where(x => x[resultSetColumn].ToString().EndsWith(pResultSetIdentifier)).ToList();
        }

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

        public DataColumn GetResultSetColumn(DataTable pTable)
        {
            ArgumentNullException.ThrowIfNull(pTable);

            DataColumn? result = pTable.Columns.Cast<DataColumn>().FirstOrDefault(x => string.Equals(x.ColumnName, ResultSetColumnName, StringComparison.InvariantCultureIgnoreCase));
            if (result == null) throw new Exception("Resultset column not found.");

            return result;
        }

        public string RemoveSuffix(string pString)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pString);

            var splitString = pString.Split(Delimiter).ToList();
            splitString.Reverse();
            splitString.RemoveAt(0);
            splitString.Reverse();
            return string.Join(Delimiter, splitString);
        }

        public string GetSuffixWithDelimiter(string pString)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pString);

            var splitString = pString.Split(Delimiter).ToList();
            splitString.Reverse();
            return string.Format($"{Delimiter}{splitString[0]}");
        }

    }
}
