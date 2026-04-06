"""Department API routes — location-based views.

Scoped to companies linked via batch_tags (actual scraped data),
NOT the full 16M+ sirene import table.
"""

from fastapi import APIRouter, Request

from fortress.api.db import fetch_all
from fortress.api.sql_helpers import merged_contacts_cte

router = APIRouter(prefix="/api/departments", tags=["departments"])


# French department names lookup
_DEPT_NAMES = {
    "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes",
    "09": "Ariège", "10": "Aube", "11": "Aude", "12": "Aveyron",
    "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal", "16": "Charente",
    "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze", "21": "Côte-d'Or",
    "22": "Côtes-d'Armor", "23": "Creuse", "24": "Dordogne", "25": "Doubs",
    "26": "Drôme", "27": "Eure", "28": "Eure-et-Loir", "29": "Finistère",
    "2A": "Corse-du-Sud", "2B": "Haute-Corse",
    "30": "Gard", "31": "Haute-Garonne", "32": "Gers", "33": "Gironde",
    "34": "Hérault", "35": "Ille-et-Vilaine", "36": "Indre", "37": "Indre-et-Loire",
    "38": "Isère", "39": "Jura", "40": "Landes", "41": "Loir-et-Cher",
    "42": "Loire", "43": "Haute-Loire", "44": "Loire-Atlantique", "45": "Loiret",
    "46": "Lot", "47": "Lot-et-Garonne", "48": "Lozère", "49": "Maine-et-Loire",
    "50": "Manche", "51": "Marne", "52": "Haute-Marne", "53": "Mayenne",
    "54": "Meurthe-et-Moselle", "55": "Meuse", "56": "Morbihan", "57": "Moselle",
    "58": "Nièvre", "59": "Nord", "60": "Oise", "61": "Orne",
    "62": "Pas-de-Calais", "63": "Puy-de-Dôme", "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales", "67": "Bas-Rhin",
    "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône", "71": "Saône-et-Loire",
    "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie", "75": "Paris",
    "76": "Seine-Maritime", "77": "Seine-et-Marne", "78": "Yvelines",
    "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne",
    "83": "Var", "84": "Vaucluse", "85": "Vendée", "86": "Vienne",
    "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne", "90": "Territoire de Belfort",
    "91": "Essonne", "92": "Hauts-de-Seine", "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne", "95": "Val-d'Oise",
    "971": "Guadeloupe", "972": "Martinique", "973": "Guyane",
    "974": "La Réunion", "976": "Mayotte",
}


@router.get("")
async def list_departments(request: Request):
    """List departments with counts & quality — scoped to scraped companies only."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND qt.workspace_id = %s"
        ws_params = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT qt2.siren FROM batch_tags qt2')}
        SELECT
            co.departement,
            COUNT(DISTINCT co.siren) AS company_count,
            COUNT(DISTINCT CASE WHEN ct.phone IS NOT NULL THEN co.siren END) AS with_phone,
            COUNT(DISTINCT CASE WHEN ct.email IS NOT NULL THEN co.siren END) AS with_email,
            COUNT(DISTINCT CASE WHEN ct.website IS NOT NULL THEN co.siren END) AS with_website
        FROM batch_tags qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN merged_contacts ct ON ct.siren = co.siren
        WHERE co.departement IS NOT NULL {ws_filter}
        GROUP BY co.departement
        ORDER BY co.departement
    """, ws_params if ws_params else None)
    result = []
    for r in rows:
        dept = r["departement"]
        total = r["company_count"] or 1
        result.append({
            **r,
            "department_name": _DEPT_NAMES.get(dept, dept),
            "phone_pct": round(100 * (r["with_phone"] or 0) / total),
            "email_pct": round(100 * (r["with_email"] or 0) / total),
            "website_pct": round(100 * (r["with_website"] or 0) / total),
        })
    return result


@router.get("/{dept}")
async def get_department_detail(dept: str, request: Request):
    """Get enriched companies in this department."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND qt.workspace_id = %s"
        ws_params = (dept, user.workspace_id)
    else:
        ws_filter = ""
        ws_params = (dept,)

    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT qt2.siren FROM batch_tags qt2')}
        SELECT DISTINCT ON (co.siren)
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.ville, co.code_postal,
            ct.phone, ct.email, ct.website
        FROM batch_tags qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN merged_contacts ct ON ct.siren = co.siren
        WHERE co.departement = %s {ws_filter}
        ORDER BY co.siren
    """, ws_params)
    return {
        "department": dept,
        "department_name": _DEPT_NAMES.get(dept, dept),
        "companies": rows or []
    }


@router.get("/{dept}/jobs")
async def get_department_jobs(dept: str, request: Request):
    """All jobs that have companies in this department."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params = (dept, user.workspace_id)
    else:
        ws_filter = ""
        ws_params = (dept,)

    rows = await fetch_all(f"""
        SELECT
            sj.batch_id, sj.batch_name, sj.status,
            sj.total_companies, sj.companies_scraped,
            sj.triage_black, sj.triage_green, sj.triage_yellow, sj.triage_red,
            sj.wave_current, sj.wave_total,
            sj.created_at, sj.updated_at,
            COUNT(DISTINCT co.siren) AS companies_in_dept
        FROM batch_data sj
        JOIN batch_tags qt ON qt.batch_name = sj.batch_name
                           OR qt.batch_name = sj.batch_id
        JOIN companies co ON co.siren = qt.siren AND co.departement = %s
        WHERE sj.status != 'deleted' {ws_filter}
        GROUP BY sj.id
        ORDER BY sj.created_at DESC
    """, ws_params)
    return rows
