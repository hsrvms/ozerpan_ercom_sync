import frappe
from frappe import _

from ozerpan_ercom_sync.custom_api.utils import generate_logger, get_machine_name, get_machine_number
from ozerpan_ercom_sync.utils import get_mysql_connection


@frappe.whitelist()
def sync_tes_detay():
    logger_dict = generate_logger("tesdetay_sync")
    logger = logger_dict["logger"]

    try:
        logger.info("Starting synchronizing TesDetay.")

        data = get_tesdetay_data()
        synced_count = 0

        for row in data:
            if frappe.db.exists("TesDetay", {"sayac": row.get("SAYAC")}):
                continue

            barcode = generate_barcode(
                araba_no=row.get("ARABANO"),
                eksen=row.get("EKSEN"),
                model=row.get("MODEL"),
                olcu=row.get("OLCU"),
                rc=row.get("RC"),
                stok_kodu=row.get("STOKKODU"),
                yer_no=row.get("YERNO"),
            )

            td = frappe.new_doc("TesDetay")
            # Map row data to TesDetay fields
            field_mappings = {
                "oto_no": "OTONO",
                "siparis_no": "SIPARISNO",
                "cari_kod": "CARIKOD",
                "poz_no": "POZNO",
                "stok_kodu": "STOKKODU",
                "model": "MODEL",
                "olcu": "OLCU",
                "pozisyon": "POZISYON",
                "aci1": "ACI1",
                "aci2": "ACI2",
                "adet": "ADET",
                "ercom": "ERCOM",
                "sayac": "SAYAC",
                "montaj_yeri": "MONTAJYERI",
                "kasa_no": "KASANO",
                "yer_no": "YERNO",
                "kanat_no": "KANATNO",
                "araba_no": "ARABANO",
                "rc": "RC",
                "program_no": "PROGRAMNO",
                "islem": "ISLEM",
                "bayi_adi": "BAYIADI",
                "eksen": "EKSEN",
                "yukseklik": "YUKSEKLIK",
                "sol_ic": "SOLIC",
                "sag_ic": "SAGIC",
                "orta": "ORTA",
                "da_kapi": "DAKAPI",
                "ds_kodu": "DSKODU",
                "ds_boyu": "DSBOYU",
                "profil_tipi": "PROFILTIPI",
                "hesap_kodu": "HESAPKODU",
                "esiksiz": "ESIKSIZ",
                "wc": "WC",
                "kanat_index": "KANATINDEX",
                "sanal_adet": "SANALADET",
                "aciklama": "ACIKLAMA",
                "uretim_sayac": "URETIMSAYAC",
                # "makina_no": "MAKINANO",
            }

            for field, key in field_mappings.items():
                setattr(td, field, row.get(key))

            machine_no = get_machine_number(td.oto_no, logger)
            machine_name = get_machine_name(machine_no)

            td.makina_no = machine_name
            td.barkod = barcode
            td.insert()
            synced_count += 1
            logger.info(f"Record {td.sayac} synchronized successfully")

        logger.info(f"Synchronized {synced_count} records successfully")
        return {"status": "ok", "message": _("TesDetay synchronized successfully.")}

    except (frappe.ValidationError, Exception) as e:
        error_message = f"Error during sync: {str(e)}"
        logger.error(error_message)
        frappe.throw(error_message)


def get_tesdetay_data():
    with get_mysql_connection() as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT *
                FROM dbtesdetay
                ORDER BY OTONO DESC
                LIMIT 100
            """
            cursor.execute(query)
            return cursor.fetchall()


def generate_barcode(araba_no, yer_no, stok_kodu, rc, model, olcu, eksen):
    ADJUSTMENT = 6
    MODELS_WITH_ADJUSTMENT = {"KANAT", "KASA"}
    PAD_LENGTH = 2
    MEASUREMENT_LENGTH = 4

    def pad_value(value, length=PAD_LENGTH, leading_zero=False):
        value_str = str(value)
        if len(value_str) < length:
            return ("0" + value_str) if leading_zero else (value_str + "0")
        return value_str

    def process_measurement(value):
        if value is None:
            return 0
        adjustment = ADJUSTMENT if model in MODELS_WITH_ADJUSTMENT else 0
        return max(0, value - adjustment)

    # Format input values
    araba_no_padded = pad_value(araba_no)
    yer_no_padded = pad_value(yer_no, leading_zero=True)

    # Process and format measurements
    olcu_processed = str(int(process_measurement(olcu))).rjust(MEASUREMENT_LENGTH, "0")
    eksen_processed = str(int(process_measurement(eksen))).rjust(MEASUREMENT_LENGTH, "0")

    # Generate barcode with consistent spacing
    return f"K{araba_no_padded}{yer_no_padded}{stok_kodu}   {rc}{olcu_processed}00{eksen_processed}00"
