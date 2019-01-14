package su.gnd.clickhouse.mutations;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class App {
    private static final int TEST_DATA_SIZE     = 1_000_000;
    private static final int REMOVE_BATCH_SIZE  = 500;

    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

        Connection con = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:8123");
        // Подготовка таблицы
        System.out.println("Create table");
        dropTable(con);
        createTable(con);

        // Генерация тестовых данных (2 толбца - дата и ID документа)
        System.out.println("Generate data");
        final List<TestModel> list = generateTestData();
        System.out.println("Store data");
        storeData(con, list);
        // Удаление данных блоками по {REMOVE_BATCH_SIZE} элементов
        System.out.println("Start batch remove");
        batchRemoveData(con, list);

        int c;
        // Запрашиваем количество необработанных мутаций. Ждем пока выполнятся все.
        while ((c = countMutationsInProgress(con)) > 0) {
            System.out.println("Active mutations: " + c);
            Thread.sleep(1000);
        }
        // Запрашиваем количество документов в таблице после обработки всех мутаций. В теории должно быть 0.
        System.out.println("Count inconsistent documents: " + countTestDocuments(con));
        con.close();
    }

    /**
     * Генерация тестового набора данных размером {TEST_DATA_SIZE}
     */
    private static List<TestModel> generateTestData() {
        final List<TestModel> list = new ArrayList<>();
        final LocalDate date = LocalDate.now();
        for (int i = 0; i < TEST_DATA_SIZE; i++)
            list.add(new TestModel(date, (long)i));
        return list;
    }

    private static void dropTable(Connection con) throws Exception {
        PreparedStatement dropTable = con.prepareStatement("DROP TABLE IF EXISTS `test_mutations`");
        dropTable.execute();
        dropTable.close();
    }

    private static void createTable(Connection con) throws Exception {
        PreparedStatement createTable = con.prepareStatement("CREATE TABLE IF NOT EXISTS `test_mutations` (\n" +
                "`date`  Date," +
                "`docId` UInt64" +
                ") ENGINE MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (date)");
        createTable.execute();
        createTable.close();
    }

    private static void storeData(Connection con, List<TestModel> list) throws Exception {
        PreparedStatement insert = con.prepareStatement("INSERT INTO `test_mutations` (`date`, `docId`) VALUES (?,?)");
        for (TestModel model : list) {
            insert.setDate(1, Date.valueOf(model.getDate()));
            insert.setLong(2, model.getDocId());
            insert.addBatch();
        }
        insert.executeBatch();
        insert.close();
    }

    /**
     * Пакетное удаление данных из таблица по {REMOVE_BATCH_SIZE} элементов за раз
     */
    private static void batchRemoveData(Connection con, List<TestModel> list) throws Exception {
        // Подготавливаем уникальный набор идентификаторов документов
        final Set<Long> ids = list.stream().map(TestModel::getDocId).collect(Collectors.toSet());
        String query = ""; // Строка с запросом на удаление
        int counter = 0;
        for (Long id: ids) {
            if (query.length() == 0)
                // Подготавливаем шапку запроса
                query = "ALTER TABLE `test_mutations` DELETE WHERE docId IN (" + id;
            else
                // Добавляем элементы в блок IN через запятую
                query += "," + id;
            if (++counter == REMOVE_BATCH_SIZE) {
                query += ")";
                removeData(con, query);
                query = "";
                counter = 0;
            }
        }
        if (counter > 0) {
            query += ")";
            removeData(con, query);
        }
    }

    private static void removeData(Connection con, String query) throws Exception {
        System.out.println(query);
        PreparedStatement remove = con.prepareStatement(query);
        remove.execute();
        remove.close();
    }

    /**
     * Возвращает количество необработанных мутаций
     */
    private static int countMutationsInProgress(Connection con) throws Exception {
        int count = -1;
        PreparedStatement countMutations = con.prepareStatement("SELECT count(*) from `system`.`mutations` WHERE `is_done` = 0");
        ResultSet res =  countMutations.executeQuery();
        if (res.next())
            count = res.getInt(1);
        res.close();
        countMutations.close();
        return count;
    }

    /**
     * Возвращает количество документов в тестовой таблице
     */
    private static int countTestDocuments(Connection con) throws Exception {
        int count = -1;
        PreparedStatement countMutations = con.prepareStatement("SELECT count(*) from `test_mutations`");
        ResultSet res =  countMutations.executeQuery();
        if (res.next())
            count = res.getInt(1);
        res.close();
        countMutations.close();
        return count;
    }
}
