package su.gnd.clickhouse.mutations;

import java.time.LocalDate;

public class TestModel {
    private final LocalDate date;
    private final Long docId;

    public TestModel(LocalDate date, Long docId) {
        this.date = date;
        this.docId = docId;
    }

    public LocalDate getDate() {
        return date;
    }

    public Long getDocId() {
        return docId;
    }
}
