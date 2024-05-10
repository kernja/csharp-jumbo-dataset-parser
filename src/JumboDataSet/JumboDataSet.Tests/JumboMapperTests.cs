using JumboDataSet.Mapper;
using Microsoft.VisualStudio.TestPlatform.CrossPlatEngine.Discovery;
using System.Data;
using System.Numerics;

namespace JumboDataSet.Tests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        public DataSet CreateValidDataSet()
        {
            //use default values from mapper
            JumboMapper jm = new JumboMapper();

            //data object variables
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            DataColumn dc;
            DataRow dr;

            //shorthand the delimiter value
            string dv = jm.Delimiter;
            //create default set column
            dc = new DataColumn(jm.ResultSetColumnName); dt.Columns.Add(dc);

            //add columns for tables 1, 2, and 3
            //Table 1 has 1 Column
            dc = new DataColumn($"ANT{dv}01"); dt.Columns.Add(dc);
            //Table 2 has 2 columns
            dc = new DataColumn($"BEE{dv}02"); dt.Columns.Add(dc);
            dc = new DataColumn($"COW{dv}02"); dt.Columns.Add(dc);
            //Table 3 has 3 columns
            dc = new DataColumn($"DAY{dv}03"); dt.Columns.Add(dc);
            dc = new DataColumn($"EGG{dv}03"); dt.Columns.Add(dc);
            dc = new DataColumn($"FIG{dv}03"); dt.Columns.Add(dc);

            //create rows for tables 1 and 3, and 2 will be empty
            dr = dt.NewRow(); dr.ItemArray = [   "RESULTSET_01",      "A",    null,   null,   null,   null,   null]; dt.Rows.Add(dr);
            dr = dt.NewRow(); dr.ItemArray = [   "RESULTSET_01",      "B",    null,   null,   null,   null,   null]; dt.Rows.Add(dr);
            dr = dt.NewRow(); dr.ItemArray = [   "RESULTSET_01",      "C",    null,   null,   null,   null,   null]; dt.Rows.Add(dr);
            dr = dt.NewRow(); dr.ItemArray = [   "RESULTSET_03",      null,   null,   null,   "D",    "E",    "F"];  dt.Rows.Add(dr);

            ds.Tables.Add(dt);
            return ds;
        }

        [Test]
        public void Step1_InvalidArguments_ArgumentExceptionIsThrown()
        {
            var sut = new JumboMapper();
            Assert.Throws<ArgumentNullException>(() => sut.Step1_GetDistinctResultSetIdentifiers(null));
        }

        [Test]
        public void Step1_ValidArguments_ExpectedResultIsReturned()
        {
            var sut = new JumboMapper();
            var data = CreateValidDataSet();
            var result = sut.Step1_GetDistinctResultSetIdentifiers(data.Tables[0]);
            Assert.That(result.Count, Is.EqualTo(3));
            Assert.True(result.Contains($"{sut.Delimiter}01"));
            Assert.True(result.Contains($"{sut.Delimiter}02"));
            Assert.True(result.Contains($"{sut.Delimiter}03"));
        }

        [Test]
        [TestCase("")]
        [TestCase(" ")]
        public void Step2_InvalidArguments_ExceptionsAreThrown(string? pSuffix)
        {
            var sut = new JumboMapper();
            Assert.Throws<ArgumentNullException>(() => sut.Step2_GetResultSetColumnMappings(null, pSuffix));
            Assert.Throws<ArgumentException>(() => sut.Step2_GetResultSetColumnMappings(new DataTable(), pSuffix));

        }

        [Test]
        [TestCase("_01", 1, "ANT")]
        [TestCase("_02", 2, "BEE")]
        [TestCase("_03", 3, "DAY")]
        public void Step2_ValidArguments_ExpectedResultIsReturned(string? pSuffix, int pExpectedCount, string pOneExpectedValue)
        {
            var sut = new JumboMapper();
            var data = CreateValidDataSet();
            var result = sut.Step2_GetResultSetColumnMappings(data.Tables[0], pSuffix);

            Assert.That(result.Count, Is.EqualTo(pExpectedCount));
            Assert.True(result.Any(x => string.Equals(x.destination, pOneExpectedValue, StringComparison.InvariantCultureIgnoreCase)));
        }

        [Test]
        public void Step3_InvalidArguments_ExceptionsAreThrown()
        {
            var sut = new JumboMapper();
            Assert.Throws<ArgumentNullException>(() => sut.Step3_CreateEmptyDestinationTable(null));
        }

        [Test]
        public void Step3_ValidArguments_ExpectedResultIsReturned()
        {
            var sut = new JumboMapper();
            IList<(string, string)> data = [("BAR_01", "BAR"), ("CHI_02", "CHI")];
            var result = sut.Step3_CreateEmptyDestinationTable(data);

            Assert.That(result.Columns.Count, Is.EqualTo(2));
            Assert.That(result.Columns[0].ColumnName, Is.EqualTo("BAR"));
            Assert.That(result.Columns[1].ColumnName, Is.EqualTo("CHI"));
        }

        [Test]
        [TestCase("")]
        [TestCase(" ")]
        public void Step4_InvalidArguments_ExceptionsAreThrown(string? pSuffix)
        {
            var sut = new JumboMapper();
            Assert.Throws<ArgumentNullException>(() => sut.Step4_GetResultSetRows(null, pSuffix));
            Assert.Throws<ArgumentException>(() => sut.Step4_GetResultSetRows(new DataTable(), pSuffix));

        }

        [Test]
        [TestCase("_01", 3, 1, "A")]
        [TestCase("_02", 0, 0, null)]
        [TestCase("_03", 1, 4, "D")]
        public void Step4_ValidArguments_ExpectedResultIsReturned(string? pSuffix, int pExpectedCount, int pOneExpectedIndex, string? pOneExpectedValue)
        {
            var sut = new JumboMapper();
            var data = CreateValidDataSet();
            var result = sut.Step4_GetResultSetRows(data.Tables[0], pSuffix);

            Assert.That(result.Count, Is.EqualTo(pExpectedCount));
            Assert.True(result.Count == 0 || result[0].ItemArray[pOneExpectedIndex].Equals(pOneExpectedValue));
        }

        [Test]
        [TestCase("")]
        [TestCase(" ")]
        public void Step5_InvalidArguments_ExceptionsAreThrown(string? pSuffix)
        {
            var data = CreateValidDataSet();
            var dt = data.Tables[0];

            var sut = new JumboMapper();
            Assert.Throws<ArgumentNullException>(() => sut.Step5_SetDestinationTableValues(ref dt, null, null));
            Assert.Throws<ArgumentNullException>(() => sut.Step5_SetDestinationTableValues(ref dt, dt.Rows[0], null));
        }

        [Test]

        public void IntegrationTest_ValidArguments_TablesAreMapped()
        {
            var data = CreateValidDataSet();
            var sut = new JumboMapper();
            var sutResult = sut.Map(data);

            Assert.That(sutResult.Tables.Count, Is.EqualTo(3));
            Assert.That(sutResult.Tables[0].Rows.Count, Is.EqualTo(3));
            Assert.That(sutResult.Tables[0].Columns[0].ColumnName, Is.EqualTo("ANT"));
            Assert.That(sutResult.Tables[0].Rows[0].ItemArray[0], Is.EqualTo("A"));
            Assert.That(sutResult.Tables[0].Rows[1].ItemArray[0], Is.EqualTo("B"));
            Assert.That(sutResult.Tables[0].Rows[2].ItemArray[0], Is.EqualTo("C"));
            Assert.That(sutResult.Tables[1].Columns[0].ColumnName, Is.EqualTo("BEE"));
            Assert.That(sutResult.Tables[1].Rows.Count, Is.EqualTo(0));
            Assert.That(sutResult.Tables[2].Rows.Count, Is.EqualTo(1));
            Assert.That(sutResult.Tables[2].Columns[0].ColumnName, Is.EqualTo("DAY"));
            Assert.That(sutResult.Tables[2].Columns[1].ColumnName, Is.EqualTo("EGG"));
            Assert.That(sutResult.Tables[2].Columns[2].ColumnName, Is.EqualTo("FIG"));
            Assert.That(sutResult.Tables[2].Rows[0].ItemArray[0], Is.EqualTo("D"));
            Assert.That(sutResult.Tables[2].Rows[0].ItemArray[1], Is.EqualTo("E"));
            Assert.That(sutResult.Tables[2].Rows[0].ItemArray[2], Is.EqualTo("F"));
        }

        [Test]
        [TestCase("")]
        [TestCase(" ")]
        public void RemoveSuffix_InvalidArguments_ArgumentExceptionIsThrown(string input)
        {
            var sut = new JumboMapper();
            Assert.Throws<ArgumentException>(() => sut.RemoveSuffix(input));
        }

        [Test]
        [TestCase("FOO_1", "FOO")]
        [TestCase("FOO_02", "FOO")]
        [TestCase("FOO_003", "FOO")]
        public void RemoveSuffix_ValidArguments_ReturnsExpectedResult(string input, string target)
        {
            var sut = new JumboMapper();
            var result = sut.RemoveSuffix(input);
            Assert.IsTrue(string.Equals(result, target));
        }

        [Test]
        [TestCase("")]
        [TestCase(" ")]
        public void GetSuffixWithDelimiter_InvalidArguments_ArgumentExceptionIsThrown(string input)
        {
            var sut = new JumboMapper();
            Assert.Throws<ArgumentException>(() => sut.GetSuffixWithDelimiter(input));
        }

        [Test]
        [TestCase("FOO_1", "_1")]
        [TestCase("FOO_02", "_02")]
        [TestCase("FOO_003", "_003")]
        public void GetSuffixWithDelimiter_ValidArguments_ReturnsExpectedResult(string input, string target)
        {
            var sut = new JumboMapper();
            var result = sut.GetSuffixWithDelimiter(input);
            Assert.IsTrue(string.Equals(result, target));
        }
    }
}