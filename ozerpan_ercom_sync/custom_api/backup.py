def sync_items(logger) -> dict[str, str]:
    """
    Synchronizes items from MySQL database to Frappe.

    Connects to MySQL database, retrieves records from dbpoz table ordered by PozID desc
    with a limit of 2 records, and creates corresponding Item and BOM records in Frappe
    if they don't already exist.

    Returns:
        dict: Message indicating sync status
    """
    logger.info("Starting items sync")
    LIMIT: int = 3000

    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            query: str = f"SELECT * FROM dbpoz ORDER BY PozID DESC LIMIT {LIMIT}"
            cursor.execute(query)
            data = cursor.fetchall()

            if not data:
                logger.warning("No item data found")
                return {"message": "No data found."}

            for i, row in enumerate(data):
                percent = (i + 1) * 100 / LIMIT
                frappe.publish_progress(
                    percent,
                    title="ERCOM Item Sync",
                    description=f"Syncing item {i+1} of {LIMIT}",
                )
                item_code: str = f"{row.get('SIPARISNO')}-{row.get('POZNO')}"
                if frappe.db.exists("Item", {"item_code": item_code}):
                    logger.info(f"Item already exists: {item_code}")
                    continue
                logger.info(f"Creating new item: {item_code}")
                item_result: dict[str, str] = create_item(row, logger)
                _ = create_bom(row, item_result["docname"], logger)

    logger.info("Items sync completed")
    return {"message": "Sync Completed"}


def create_bom(row: dict, item: str, logger) -> dict[str, str]:
    """
    Creates a new Bill of Materials (BOM) record in Frappe from MySQL row data.

    Args:
        row (dict): Dictionary containing BOM data from MySQL dbpoz table
        item (str): Item docname to link BOM to

    Returns:
        dict: Message indicating success and docname of created BOM
            e.g. {"msg": "BOM created successfully.", "docname": "BOM-001"}
    """
    company: str = frappe.defaults.get_user_default("Company")
    b = frappe.new_doc("BOM")
    b.item = item
    b.company = company
    b.quantity = row.get("ADET")
    b.append(
        "items",
        {
            "item_code": item,
        },
    )
    b.insert(ignore_permissions=True)
    logger.info(f"Created BOM for item {item}")
    return {"msg": "BOM created successfully.", "docname": b.name}


def create_item(row: dict, logger) -> dict[str, str]:
    """
    Creates a new Item record in Frappe from MySQL row data.

    Args:
        row (dict): Dictionary containing item data from MySQL dbpoz table

    Returns:
        dict: Message indicating success and docname of created Item
            e.g. {"msg": "Item created successfully.", "docname": "ITEM-001"}
    """
    i = frappe.new_doc("Item")
    custom_code: str = f"{row.get('SIPARISNO')}-{row.get('POZNO')}"
    i.item_code = custom_code
    i.item_name = custom_code
    i.item_group = "All Item Groups"
    i.stock_uom = "Unit"
    i.valuation_rate = row.get("TUTAR")
    i.description = row.get("ACIKLAMA")
    i.custom_serial = row.get("SERI")
    i.custom_width = row.get("GENISLIK")
    i.custom_height = row.get("HEIGHT")
    i.custom_color = row.get("RENK")
    i.custom_quantity = row.get("ADET")
    i.custom_remarks = row.get("NOTLAR")
    i.custom_poz_id = row.get("PozID")
    i.insert(ignore_permissions=True)
    logger.info(f"Created item {custom_code}")
    return {"msg": "Item created successfully.", "docname": i.name}
