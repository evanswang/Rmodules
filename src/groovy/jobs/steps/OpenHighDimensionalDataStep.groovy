package jobs.steps

import grails.util.Holders
import jobs.misc.AnalysisConstraints
import jobs.UserParameters
import jobs.misc.Hacks
import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.HighDimensionDataTypeResource
import org.transmartproject.core.dataquery.highdim.assayconstraints.AssayConstraint
import org.transmartproject.core.dataquery.highdim.dataconstraints.DataConstraint
import org.transmartproject.core.dataquery.highdim.projections.Projection
import org.transmartproject.db.dataquery.highdim.DefaultHighDimensionTabularResult
import org.transmartproject.db.dataquery.highdim.RepeatedEntriesCollectingTabularResult
import org.transmartproject.db.dataquery.highdim.mrna.ProbeRow

class OpenHighDimensionalDataStep implements Step {

    final String statusName = 'Gathering Data'

    /* in */
    UserParameters params
    HighDimensionDataTypeResource dataTypeResource
    AnalysisConstraints analysisConstraints

    /* out */
    Map<List<String>, TabularResult> results = [:]

    @Override
    void execute() {
        try {
            List<String> ontologyTerms = extractOntologyTerms()
            extractPatientSets().eachWithIndex { resultInstanceId, index ->
                ontologyTerms.each { ontologyTerm ->
                    String seriesLabel = ontologyTerm.split('\\\\')[-1]
                    List<String> keyList = ["S" + (index + 1), seriesLabel]
                    results[keyList] = fetchSubset(resultInstanceId, ontologyTerm)
                }
            }
        } catch(Throwable t) {
            results.values().each { it.close() }
            throw t
        }
    }

    private List<String> extractOntologyTerms() {
        analysisConstraints.assayConstraints.remove('ontology_term').collect {
            Hacks.createConceptKeyFrom(it.term)
        }
    }

    private List<Integer> extractPatientSets() {
        analysisConstraints.assayConstraints.remove("patient_set").grep()
    }

    private TabularResult fetchSubset(Integer patientSetId, String ontologyTerm) {
        if(!ConfigurationHolder.config.org.transmart.kv.enable) {
            List<DataConstraint> dataConstraints = analysisConstraints['dataConstraints'].
                    collect { String constraintType, values ->
                        if (values) {
                            dataTypeResource.createDataConstraint(values, constraintType)
                        }
                    }.grep()

            List<AssayConstraint> assayConstraints = analysisConstraints['assayConstraints'].
                    collect { String constraintType, values ->
                        if (values) {
                            dataTypeResource.createAssayConstraint(values, constraintType)
                        }
                    }.grep()

            assayConstraints.add(
                    dataTypeResource.createAssayConstraint(
                            AssayConstraint.PATIENT_SET_CONSTRAINT,
                            result_instance_id: patientSetId))

            assayConstraints.add(
                    dataTypeResource.createAssayConstraint(
                            AssayConstraint.ONTOLOGY_TERM_CONSTRAINT,
                            concept_key: ontologyTerm))

            Projection projection = dataTypeResource.createProjection([:],
                    analysisConstraints['projections'][0])

            dataTypeResource.retrieveData(assayConstraints, dataConstraints, projection)
        } else {
            // @wsc the following 2 lines are kept for gene names and data type
            List geneList = null
            if (analysisConstraints['dataConstraints']['search_keyword_ids'] != null)
                geneList = analysisConstraints['dataConstraints']['search_keyword_ids']['keyword_ids']
            System.err.println("@wsc print geneList element type ***************** " + geneList[0].toString());
            String dataType = analysisConstraints['projections']
            System.err.println("kv api enabled **************************")
            System.err.println(patientSetId.toString() + ":" + dataType.substring(2, dataType.length() - 2));
            System.err.println(ontologyTerm);
            System.err.println(geneList.toListString());
            System.err.println("************* printing end **************************")
            new DefaultHighDimensionTabularResult(
                    rowsDimensionLabel:    patientSetId.toString() + ":" + dataType.substring(2, dataType.length() - 2),
                    columnsDimensionLabel: ontologyTerm,
                    indicesList:           geneList,
                    results:               null,
                    allowMissingAssays:    true,
                    assayIdFromRow:        { it[0].assay.id },
                    inSameGroup:           { a, b -> a.probeId == b.probeId && a.geneSymbol == b.geneSymbol },
                    finalizeGroup:         { List list ->
                        def firstNonNullCell = list.find()
                        new ProbeRow(
                                probe:         firstNonNullCell[0].probeName,
                                geneSymbol:    firstNonNullCell[0].geneSymbol,
                                geneId:        firstNonNullCell[0].geneId,
                                assayIndexMap: assayIndexMap,
                                data:          list.collect { projection.doWithResult it?.getAt(0) }
                        )
                    }
            )
        }
    }
}

