package jobs.steps

import au.com.bytecode.opencsv.CSVWriter
import jobs.UserParameters
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.springframework.context.ApplicationContext
import org.transmart.db.dataquery.SQLModule
import org.transmart.db.dataquery.mrna.ExpressionRecord
import org.transmart.db.dataquery.mrna.KVMrnaModule
import org.transmartproject.core.dataquery.DataRow
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.AssayColumn

abstract class AbstractDumpHighDimensionalDataStep extends AbstractDumpStep {

    final String statusName = 'Dumping high dimensional data'

    /* true if computeCsvRow is to be called once per (row, column),
       false to called only once per row */
    boolean callPerColumn = true

    // @wsc add to get sql connection
    ApplicationContext ctx = org.codehaus.groovy.grails.web.context.ServletContextHolder.getServletContext().getAttribute(org.codehaus.groovy.grails.web.servlet.GrailsApplicationAttributes.APPLICATION_CONTEXT)
    def dataSource = ctx.getBean('dataSource')

    File temporaryDirectory
    Closure<Map<List<String>, TabularResult>> resultsHolder
    UserParameters params

    Map<List<String>, TabularResult> getResults() {
        resultsHolder()
    }

    @Override
    void execute() {
        try {
            writeDefaultCsv results, csvHeader
        } finally {
            // results.values().each { it?.close() }
        }
    }

    abstract protected computeCsvRow(String subsetName,
                                     String seriesName,
                                     DataRow row,
                                     AssayColumn column,
                                     Object cell)

    abstract List<String> getCsvHeader()

    protected String getRowKey(String subsetName, String seriesName, String patientId) {
        if (params.doGroupBySubject == "true") {
            return [subsetName, patientId, seriesName].join("_")
        }
        return [subsetName, seriesName, patientId].join("_")
    }

    private void withDefaultCsvWriter(Closure constructFile) {
        File output = new File(temporaryDirectory, outputFileName)
        output.createNewFile()
        output.withWriter { writer ->
            CSVWriter csvWriter = new CSVWriter(writer, '\t' as char)
            constructFile.call(csvWriter)
        }
    }

    /* nextRow is a closure with this signature:
     * (String subsetName, DataRow row, Long rowNumber, AssayColumn column, Object cell) -> List<Object> csv row
     */
    private void writeDefaultCsv(Map<List<String>, TabularResult<AssayColumn, DataRow<AssayColumn, Object>>> results,
                                 List<String> header) {
        System.err.println(System.nanoTime() + "@wsc revise csv writing ****************************")
        withDefaultCsvWriter { CSVWriter csvWriter ->
            csvWriter.writeNext header as String[]
            if (!ConfigurationHolder.config.org.transmart.kv.enable) {
                results.keySet().each { key ->
                    doSubset(key, csvWriter)
                }
            } else {
                results.keySet().each { key ->
                    System.err.println(System.nanoTime() + "@wsc print result key set ****************************" + key.toString())
                    doSubsetKV(key, csvWriter)
                }
            }
        }

    }

    private void doSubsetKV (List<String> resultsKey, CSVWriter csvWriter) {
        // @wsc add kv data query function
        def tabularResult = results[resultsKey]
        if (!tabularResult) {
            return
        }

        String subsetName = resultsKey[0]
        String seriesName = resultsKey[1]

        String combinedStr = tabularResult.getRowsDimensionLabel()
        String resultInstanceId = combinedStr.substring(0, combinedStr.indexOf(":"))
        String dataType = combinedStr.substring(combinedStr.indexOf(":") + 1, combinedStr.length())
        String ontologyTerm = tabularResult.getColumnsDimensionLabel()
        ontologyTerm = ontologyTerm.substring(StringUtils.ordinalIndexOf(ontologyTerm, "\\", 3))
        List geneList = tabularResult.getIndicesList()


        //Sql command used to retrieve Assay IDs.
        System.err.println("before init sql **************************** " + ontologyTerm)
        List<String> patientList = SQLModule.getPatients(resultInstanceId)
        Map<String, String> trial_conceptcd = SQLModule.getTrialandConceptCD(ontologyTerm)
        String trialName = trial_conceptcd.get("study_name");
        String conceptCD = trial_conceptcd.get("concept_cd");
        List<ExpressionRecord> kvResults = null
        try {
            // @wsc CSV writer
            KVMrnaModule kvMrnaModule = new KVMrnaModule("microarray-subject", dataType)
            System.err.println(System.nanoTime() + "@wsc launch hbase query **************************** ")
            if (geneList == null) {
                kvResults = kvMrnaModule.getRecord(trialName, patientList, conceptCD)
            } else {
                Map probes2GeneMap = SQLModule.getProbes2GeneMap(geneList)
                kvResults = kvMrnaModule.getRecord(trialName, patientList, conceptCD, new ArrayList<String>(probes2GeneMap.keySet()))
            }

            System.err.println(System.nanoTime() + "@wsc hbase query end **************************** ")
            kvResults.each { kvRecord ->
                // gene_id not complete, value is only raw type
                csvWriter.writeNext(
                        [getRowKey(subsetName, seriesName, kvRecord.getPatientID()), kvRecord.getValue(), kvRecord.getProbeset(), probes2GeneMap.get(kvRecord.getProbeset())] as String[]
                )
            }
        } catch (Exception e) {
            System.err.println(System.nanoTime() + "@wsc got errors when launching hbase query **************************** " + e.getMessage())
            e.printStackTrace()
        }
    }

    private void doSubset(List<String> resultsKey, CSVWriter csvWriter) {

        def tabularResult = results[resultsKey]
        if (!tabularResult) {
            return
        }

        String subsetName = resultsKey[0]
        String seriesName = resultsKey[1]

        def assayList = tabularResult.indicesList

        tabularResult.each { DataRow row ->
            if (callPerColumn) {
                assayList.each { AssayColumn assay ->
                    if (row[assay] == null) {
                        return
                    }

                    def csvRow = computeCsvRow(subsetName,
                            seriesName,
                            row,
                            assay,
                            row[assay])

                    csvWriter.writeNext csvRow as String[]
                }
            } else {
                def csvRow = computeCsvRow(subsetName,
                        seriesName,
                        row,
                        null,
                        null)

                csvWriter.writeNext csvRow as String[]
            }
        }
    }

}
