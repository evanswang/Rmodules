package jobs.steps

import org.transmartproject.core.dataquery.DataRow
import org.transmartproject.core.dataquery.highdim.AssayColumn

class ValueGroupDumpDataStep extends AbstractDumpHighDimensionalDataStep {

    @Override
    protected computeCsvRow(String subsetName,
                            String seriesName,
                            DataRow row,
                            Long rowNumber,
                            AssayColumn column,
                            Object cell) {
        [
                getRowKey(subsetName, seriesName, column.patientInTrialId),
                row[column],
                row.label
        ]
    }

    final List<String> csvHeader = [ 'PATIENT_NUM', 'VALUE', 'GROUP' ]
}
