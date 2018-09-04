package com.assistant.pos.migrator;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

@SuppressWarnings("WeakerAccess")
public class Migrator {
    private ConnUtils connUtils = new ConnUtils();
    private JDBCloner dbCloner = new JDBCloner();

    public void migrate(String from, String to) {
        JdbcTemplate jtFrom = connUtils.getJdbcTemplate(from);
        JdbcTemplate jtTo = connUtils.getJdbcTemplate(to);

        dbCloner.cloneRows("provider", "provider", jtFrom, jtTo);
        dbCloner.cloneRows("measurement_unit", "measurement_unit", jtFrom, jtTo);
        dbCloner.cloneRows("brand", "brand", jtFrom, jtTo);
        dbCloner.cloneRows("purchase", "purchase", jtFrom, jtTo);
        migrateProducts(jtFrom, jtTo);
        dbCloner.cloneRows("purchase_price", "purchase_price", jtFrom, jtTo);
        dbCloner.cloneRows("sale_price", "sale_price", jtFrom, jtTo);
        migrateExistences(jtFrom, jtTo);
        migratePurchaseHasProducts(jtFrom, jtTo);
        migrateSales(jtFrom, jtTo);
        migrateSaleContents(jtFrom, jtTo);
    }

    private void migrateProducts(JdbcTemplate jtFrom, JdbcTemplate jtTo) {
        System.out.println("\n\n");

        List<Map<String, Object>> products = dbCloner.getRowsInTable(jtFrom, "product");
        int count = 0;
        for (Map<String, Object> product : products) {
            Object id = product.get("id");
            Object brandId = product.get("brand_id");
            Object unitId = product.get("measurement_unit_id");
            Object name = product.get("name");
            Object code = product.get("code");
            Object description = product.get("description");
            Object minExistences = product.get("minimal_existences");
            Object createdAt = product.get("createdAt");
            Object updatedAt = product.get("updatedAt");

            String insert = "INSERT INTO product VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?);";

            Object[] params = new Object[9];
            params[0] = id;
            params[1] = name;
            params[2] = code;
            params[3] = description;
            params[4] = minExistences;
            params[5] = brandId;
            params[6] = unitId;
            params[7] = createdAt;
            params[8] = updatedAt;
            jtTo.update(insert, params);
            System.out.println("Inserted " + ++count + " of " + products.size());
        }

        System.out.println("All products migrated");
    }

    private void migrateExistences(JdbcTemplate jtFrom, JdbcTemplate jtTo) {
        System.out.println("\n\n");
        int count = 0;

        List<Map<String, Object>> products = dbCloner.getRowsInTable(jtTo, "product");
        for (Map<String, Object> product : products) {
            Object id = product.get("id");

            Map<String, Object> existences = jtFrom.queryForMap(
                    "SELECT\n" +
                            "  existence.product_id,\n" +
                            "  SUM(IFNULL(1 - CONSUMED.quantity, 1)) AS stock \n" +
                            "FROM existence\n" +
                            "  LEFT JOIN (\n" +
                            "      SELECT existence_id, SUM(IFNULL(partial_quantity, 1)) AS quantity\n" +
                            "      FROM sale_has_existence\n" +
                            "      GROUP BY existence_id\n" +
                            "    ) CONSUMED\n" +
                            "    ON CONSUMED.existence_id = existence.id \n" +
                            "WHERE existence.product_id = ?",
                    id
            );
            Object stock = existences.get("stock");
            if (stock == null) {
                stock = 0;
            }

            String update = "UPDATE product SET existences = ? WHERE id = ?;";
            jtTo.update(update, stock, id);
            System.out.println("Migrated existences: " + ++count + " of " + products.size());
        }

        System.out.println("\nExistences migrated successfully");
    }

    private void migratePurchaseHasProducts(JdbcTemplate jtFrom, JdbcTemplate jtTo) {
        System.out.println("\n\nNow preparing to migrate purchase has products");
        int count = 0;

        List<Map<String, Object>> existences = jtFrom.queryForList("" +
                "SELECT\n" +
                "  purchase_price_id,\n" +
                "  product_id,\n" +
                "  purchase_id,\n" +
                "  COUNT(*) as count\n" +
                "FROM existence\n" +
                "GROUP BY purchase_price_id, product_id, purchase_id");
        for (Map<String, Object> existence : existences) {
            Object priceId = existence.get("purchase_price_id");
            Object productId = existence.get("product_id");
            Object purchaseId = existence.get("purchase_id");
            Object quantity = existence.get("count");

            String update = "INSERT INTO purchase_has_product VALUES (null, ?, ?, ?, ?, date('now'), date('now'));";
            jtTo.update(update, purchaseId, productId, priceId, quantity);
            System.out.println("Migrated purchase contents: " + ++count + " of " + existences.size());
        }

        System.out.println("\nPurchase contents migrated successfully");
    }

    private void migrateSales(JdbcTemplate jtFrom, JdbcTemplate jtTo) {
        System.out.println("\n\nNow preparing to migrate sales");
        int count = 0;

        List<Map<String, Object>> sales = dbCloner.getRowsInTable(jtFrom, "sale");
        for (Map<String, Object> sale : sales) {
            Object id = sale.get("id");
            Object total = sale.get("total");
            Object date = sale.get("date");
            Object createdAt = sale.get("createdAt");
            Object updatedAt = sale.get("updatedAt");

            String insert = "INSERT INTO sale VALUES(?, ?, ?, ?, ?);";
            jtTo.update(insert, id, total, date, createdAt, updatedAt);
            System.out.println("Inserted " + ++count + " of " + sales.size());
        }

        System.out.println("\nSales migrated successfully");
    }

    private void migrateSaleContents(JdbcTemplate jtFrom, JdbcTemplate jtTo) {
        System.out.println("\n\nNow preparing to migrate sale contents");
        int count = 0;

        List<Map<String, Object>> sales = dbCloner.getRowsInTable(jtTo, "sale");
        for (Map<String, Object> sale : sales) {
            count++;
            Object id = sale.get("id");

            List<Map<String, Object>> contents = jtFrom.queryForList("\n" +
                    "SELECT\n" +
                    "  sale_price_id,\n" +
                    "  self_consumption,\n" +
                    "  SUM(ifnull(partial_quantity, 1)) AS quantity \n" +
                    "FROM sale_has_existence \n" +
                    "INNER JOIn existence e on sale_has_existence.existence_id = e.id \n" +
                    "WHERE sale_id = ? " +
                    "GROUP BY sale_price_id, self_consumption, e.product_id", id);
            for (Map<String, Object> content : contents) {
                Object priceId = content.get("sale_price_id");
                Object selfConsumption = content.get("self_consumption");
                Object quantity = content.get("quantity");

                String insert = "INSERT INTO sale_has_product VALUES(null, ?, ?, ?, ?, date('now'), date('now'));";
                jtTo.update(insert, id, priceId, selfConsumption, quantity);
                System.out.println("Inserted content of sale " + count + " of " + sales.size());
            }
        }

        System.out.println("\nSale contents migrated successfully");
    }
}
