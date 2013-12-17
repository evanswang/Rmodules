package jobs.steps

import au.com.bytecode.opencsv.CSVWriter
import org.transmartproject.core.dataquery.DataRow
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.AssayColumn

abstract class AbstractDumpHighDimensionalDataStep implements Step {

    final String statusName = null

    File temporaryDirectory
    Closure<Map<String, TabularResult>> resultsHolder

    Map<String, TabularResult> getResults() {
        resultsHolder()
    }

    @Override
    void execute() {
        try {
            writeDefaultCsv results, csvHeader
        } finally {
            results.values().each { it?.close() }
        }
    }

    abstract protected computeCsvRow(String subsetName,
                                     String seriesName,
                                     DataRow row,
                                     Long rowNumber,
                                     AssayColumn column,
                                     Object cell)

    abstract List<String> getCsvHeader()

    protected String getRowKey(String subsetName, String seriesName, String patientId) {
        return [subsetName, seriesName, patientId].join("_")
    }

    private void withDefaultCsvWriter(Closure constructFile) {
        File output = new File(temporaryDirectory, 'outputfile')
        output.createNewFile()
        output.withWriter { writer ->
            CSVWriter csvWriter = new CSVWriter(writer, '\t' as char)
            constructFile.call(csvWriter)
        }
    }

    /* nextRow is a closure with this signature:
     * (String subsetName, DataRow row, Long rowNumber, AssayColumn column, Object cell) -> List<Object> csv row
     */
    private void writeDefaultCsv(Map<String, TabularResult<AssayColumn, DataRow<AssayColumn, Object>>> results,
                                 List<String> header) {


        withDefaultCsvWriter { CSVWriter csvWriter ->

            csvWriter.writeNext header as String[]

            results.keySet().each { key ->
                doSubset(key, csvWriter)
            }
        }
    }

    private void doSubset (String resultsKey, CSVWriter csvWriter) {

        def tabularResult = results[resultsKey]
        if (!tabularResult) {
            return
        }

        String[] split = resultsKey.split("_")
        String subsetName = split[0]
        String seriesName = split[1]

        def assayList = tabularResult.indicesList

        long i = 0
        tabularResult.each { DataRow row ->
            assayList.each { AssayColumn assay ->
                if (!row[assay]) {
                    return
                }

                def csvRow = computeCsvRow(subsetName,
                                           seriesName,
                                           row,
                                           i++,
                                           assay,
                                           row[assay])

                csvWriter.writeNext csvRow as String[]
            }
        }
    }

}
