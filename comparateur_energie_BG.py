from google.cloud import bigquery
from datetime import datetime, timezone, date
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup, Comment
import pandas as pd
import re
import time

BQ_PROJECT = "ohm-benchmark-pricing"
BQ_TABLE = "ohm-benchmark-pricing.benchmark_pricing.offres_pricing"

def delete_today_partition(client: bigquery.Client, event_date: date):
    query = f"""
    DELETE FROM `{BQ_TABLE}`
    WHERE event_date = @event_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_date", "DATE", event_date)
        ]
    )
    client.query(query, job_config=job_config).result()
    print(f"🧹 Partition supprimée pour event_date={event_date}")

def write_to_bigquery_overwrite_today(df: pd.DataFrame):
    client = bigquery.Client(project=BQ_PROJECT)

    # date du run (UTC) -> cohérent et stable
    run_date = datetime.now(timezone.utc).date()
    run_ts = datetime.now(timezone.utc)

    # 1) purge du jour
    delete_today_partition(client, run_date)

    # 2) préparation DF
    df["event_date"] = run_date
    df["ingestion_ts"] = run_ts

    df = df.rename(columns={
        "Profil ID": "profil_id",
        "Ville": "ville",
        "Type d’énergie": "type_energie",
        "Profil": "profil",
        "Fournisseur": "fournisseur",
        "Offre": "offre",
        "Puissance": "puissance",
        "Prix de l’abonnement TTC": "prix_abonnement_ttc",
        "Prix HP kWh TTC": "prix_hp_kwh_ttc",
        "Prix HC kWh TTC": "prix_hc_kwh_ttc",
        "Prix total TTC": "prix_total_ttc",
        "Prix total TTC remisé": "prix_total_ttc_remise",
        "% vert": "pourcentage_vert",
        "Mécanique": "mecanique",
        "Niveau de stabilité": "niveau_stabilite",
        "Positionnement": "positionnement",
        "Positionnement prix remisé": "positionnement_prix_remise",
    })

    # cast (pour éviter l'erreur float->string sur % vert)
    if "pourcentage_vert" in df.columns:
        df["pourcentage_vert"] = df["pourcentage_vert"].astype(str)

    # garder uniquement les colonnes attendues (évite les surprises)
    cols = [
        "event_date","ingestion_ts","profil_id","ville","type_energie","profil",
        "fournisseur","offre","puissance",
        "prix_abonnement_ttc","prix_hp_kwh_ttc","prix_hc_kwh_ttc",
        "prix_total_ttc","prix_total_ttc_remise",
        "pourcentage_vert","mecanique","niveau_stabilite",
        "positionnement","positionnement_prix_remise"
    ]
    df = df[[c for c in cols if c in df.columns]]

    # 3) append (après purge => équivalent overwrite du jour)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config).result()
    print(f"✅ BigQuery OK : {len(df)} lignes insérées pour event_date={run_date}")

CONFIGS = [
  {
        "profil_id": "PARIS_GAZ_14000",
        "ville": "PARIS 15",
        "code_postal": "75015",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "PARIS_GAZ_10000",
        "ville": "PARIS 15",
        "code_postal": "75015",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "PARIS_GAZ_5000",
        "ville": "PARIS 15",
        "code_postal": "75015",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "BORDEAUX_GAZ_5000",
        "ville": "BORDEAUX",
        "code_postal": "33000",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "BORDEAUX_GAZ_10000",
        "ville": "BORDEAUX",
        "code_postal": "33000",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "BORDEAUX_GAZ_14000",
        "ville": "BORDEAUX",
        "code_postal": "33000",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "STRASBOURG_GAZ_5000",
        "ville": "STRASBOURG",
        "code_postal": "67000",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "STRASBOURG_GAZ_10000",
        "ville": "STRASBOURG",
        "code_postal": "67000",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "STRASBOURG_GAZ_14000",
        "ville": "STRASBOURG",
        "code_postal": "67000",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "METZ_GAZ_5000",
        "ville": "METZ",
        "code_postal": "57000",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "METZ_GAZ_10000",
        "ville": "METZ",
        "code_postal": "57000",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "METZ_GAZ_14000",
        "ville": "METZ",
        "code_postal": "57000",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "MONTPELLIER_GAZ_5000",
        "ville": "MONTPELLIER",
        "code_postal": "34000",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "MONTPELLIER_GAZ_10000",
        "ville": "MONTPELLIER",
        "code_postal": "34000",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "MONTPELLIER_GAZ_14000",
        "ville": "MONTPELLIER",
        "code_postal": "34000",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "MONTPELLIER_Base_6kVA",
        "ville": "MONTPELLIER",
        "code_postal": "34000",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "MONTPELLIER_HPHC_9kVA",
        "ville": "MONTPELLIER",
        "code_postal": "34000",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",
    },
    {
        "profil_id": "PARIS15_Base_6kVA",
        "ville": "PARIS 15",
        "code_postal": "75015",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "PARIS15_HPHC_9kVA",
        "ville": "PARIS 15",
        "code_postal": "75015",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",
    },
    {
        "profil_id": "STRASBOURG_Base_6kVA",
        "ville": "STRASBOURG",
        "code_postal": "67000",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "STRASBOURG_HPHC_9kVA",
        "ville": "STRASBOURG",
        "code_postal": "67000",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",
    },
    {
        "profil_id": "METZ_Base_6kVA",
        "ville": "METZ",
        "code_postal": "57000",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "METZ_HPHC_9kVA",
        "ville": "METZ",
        "code_postal": "57000",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",
    },
        {
        "profil_id": "NIORT_GAZ_5000",
        "ville": "NIORT (hors centre ville)",
        "code_postal": "79000",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "NIORT_GAZ_10000",
        "ville": "NIORT",
        "code_postal": "79000 (hors centre ville)",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "NIORT_GAZ_14000",
        "ville": "NIORT",
        "code_postal": "79000 (hors centre ville)",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "NIORT_Base_6kVA",
        "ville": "NIORT (hors centre ville)",
        "code_postal": "79000",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "NIORT_HPHC_9kVA",
        "ville": "NIORT (hors centre ville)",
        "code_postal": "79000",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",

    },
    {
        "profil_id": "COLMAR_GAZ_5000",
        "ville": "COLMAR",
        "code_postal": "68000",
        "type_energie": "Gaz",
        "conso_kwh": "5000",
    },
    {
        "profil_id": "COLMAR_GAZ_10000",
        "ville": "COLMAR",
        "code_postal": "68000",
        "type_energie": "Gaz",
        "conso_kwh": "10000",
    },
    {
        "profil_id": "COLMAR_GAZ_14000",
        "ville": "COLMAR",
        "code_postal": "68000",
        "type_energie": "Gaz",
        "conso_kwh": "14000",
    },
    {
        "profil_id": "COLMAR_Base_6kVA",
        "ville": "COLMAR",
        "code_postal": "68000",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "COLMAR_HPHC_9kVA",
        "ville": "COLMAR",
        "code_postal": "68000",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",
    },
    {
        "profil_id": "CHAUNAY_Base_6kVA",
        "ville": "CHAUNAY",
        "code_postal": "86510",
        "type_energie": "Électricité",
        "profil": "Base",
        "puissance": "6",
        "conso_kwh": "2163",
    },
    {
        "profil_id": "CHAUNAY_HPHC_9kVA",
        "ville": "CHAUNAY",
        "code_postal": "86510",
        "type_energie": "Électricité",
        "profil": "HPHC",
        "puissance": "9",
        "conso_kwh": "5126",
        "ratio_hp": "57",
    },
]

# --- utilitaire pour parser les nombres FR -> float ---
def to_float_fr(s: str):
    if not s:
        return None
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None


def extraire_offres(cfg):
    # 🔧 sécurisation des clés manquantes
    ville = cfg.get("ville", "?")
    code_postal = cfg.get("code_postal", "?")
    type_energie = cfg.get("type_energie", "?")
    profil = cfg.get("profil", "—")
    puissance = cfg.get("puissance", "")

    print(f"\n⚡ Simulation pour {ville} ({code_postal}) - {type_energie} [{profil}]")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_viewport_size({"width": 1400, "height": 900})

        print("🌐 Ouverture de la page de configuration...")
        page.goto("https://comparateur-offres.energie-info.fr/compte/configuration-recherche")

        # 🍪 Accepter les cookies
        try:
            page.wait_for_selector("#tarteaucitronPersonalize", timeout=8000)
            page.click("#tarteaucitronPersonalize")
            print("🍪 Bandeau cookies accepté.")
        except:
            print("⚠️ Aucun bandeau cookies trouvé (déjà accepté ?)")

        # --- Étape 1 ---
        print("📍 Étape 1 : saisie du code postal et sélection de l’énergie")

        page.wait_for_selector("#profil_zipcode", timeout=15000)
        zipcode_input = page.locator("#profil_zipcode")
        zipcode_input.click()
        for c in code_postal:
            zipcode_input.type(c, delay=200)
        print(f"🏙️ Code postal {code_postal} saisi (saisie lente).")

        print("⏳ Attente du chargement des communes...")
        for i in range(15):
            options = page.query_selector_all("#profil_cityId option")
            if len(options) > 1:
                communes = [o.text_content().strip() for o in options if o.text_content().strip()]
                print(f"🏘️ Communes détectées : {communes}")
                break
            time.sleep(1)
        else:
            raise Exception("❌ Aucune commune détectée après 15s.")

        page.select_option("#profil_cityId", label=ville)
        print(f"🏘️ Commune sélectionnée : {ville}")

        page.wait_for_selector("label[for='profil_energyType_0']", state="visible", timeout=15000)

        if type_energie.lower().startswith("élec"):
            page.click("label[for='profil_energyType_0']")
            print("🔌 Type d’énergie : Électricité sélectionné.")
        else:
            page.click("label[for='profil_energyType_1']")
            print("🔥 Type d’énergie : Gaz sélectionné.")

        page.click("button.btn-next")
        print("➡️ Étape 1 validée (profil).")

        if type_energie.lower().startswith("élec"):
            page.wait_for_url(re.compile("compte/electricite"), timeout=30000)
        else:
            page.wait_for_url(re.compile("compte/gaz"), timeout=30000)

        # --- Étape 2 ---
        print("⚙️ Étape 2 : Remplissage du formulaire de consommation...")

        if type_energie.lower().startswith("élec"):
            page.wait_for_url(re.compile("compte/electricite"), timeout=30000)

            print("⏳ Attente de l'apparition du bouton 'Linky'...")
            page.wait_for_selector("label[for='elec_consumption_linky_0']", state="visible", timeout=30000)
            page.click("label[for='elec_consumption_linky_0']")
            print("🔌 Linky : Oui sélectionné.")
            time.sleep(1.5)

            print("⏳ Vérification de la question 'Je suis titulaire d’un contrat d’électricité'...")
            found_contract_question = False
            for i in range(15):
                try:
                    if page.is_visible("label[for='elec_consumption_hasContract_0']"):
                        page.click("label[for='elec_consumption_hasContract_0']")
                        print("📄 Contrat : Oui sélectionné.")
                        found_contract_question = True
                        break
                except:
                    pass
                time.sleep(1)
            if not found_contract_question:
                print("⚠️ Question 'Contrat d’électricité' absente (cas ELD probable).")
            time.sleep(1.5)

            print("⏳ Vérification de la question 'Je souhaite utiliser mon N°PRM'...")
            found_prm_question = False
            for i in range(15):
                try:
                    if page.is_visible("label[for='elec_consumption_knowPrm_1']"):
                        page.click("label[for='elec_consumption_knowPrm_1']")
                        print("🧾 Utilisation du PRM : Non sélectionné.")
                        found_prm_question = True
                        break
                except:
                    pass
                time.sleep(1)
            if not found_prm_question:
                print("⚠️ Question 'Utilisation PRM' absente (cas ELD probable).")
            time.sleep(1.5)

            print("⏳ Attente de la question 'Je connais la puissance de mon compteur'...")
            for i in range(30):
                try:
                    if page.is_visible("label[for='elec_consumption_knowPower_0']"):
                        page.click("label[for='elec_consumption_knowPower_0']")
                        print("⚙️ Je connais la puissance : Oui sélectionnée.")
                        break
                except:
                    pass
                time.sleep(1)
            time.sleep(1.5)

            print("⏳ Sélection de la puissance du compteur...")
            for i in range(30):
                try:
                    if page.is_visible("#elec_consumption_power"):
                        page.select_option("#elec_consumption_power", label=f"{puissance} kVA")
                        print(f"💡 Puissance sélectionnée : {puissance} kVA")
                        break
                except:
                    pass
                time.sleep(1)
            time.sleep(1.5)

            print("⏳ Sélection du type de tarification...")
            if profil.upper() == "HPHC":
                page.click("label[for='elec_consumption_type_1']")
                print("⏱️ Tarification : Heures Pleines / Heures Creuses sélectionnée.")
                time.sleep(1.5)

                # ✅ Sélection du ratio HP/HC configurable
                ratio_hp = int(cfg.get("ratio_hp", 57))  # fallback à 60%
                print(f"🔁 Ratio HP souhaité : {ratio_hp}%")
                try:
                    page.select_option("#elec_consumption_hpRatio", value=str(ratio_hp))
                    print(f"✅ Ratio HP/HC appliqué : {ratio_hp}% HP / {100-ratio_hp}% HC")
                except Exception as e:
                    print(f"⚠️ Impossible de sélectionner le ratio HP/HC ({e}) — ratio par défaut conservé.")
                time.sleep(1.5)

            else:
                page.click("label[for='elec_consumption_type_0']")
                print("💰 Tarification : Base sélectionnée.")
            time.sleep(1.5)

            page.select_option("#elec_consumption_knowConso", value="1")
            print("📈 Mode de saisie : Je connais ma consommation annuelle.")
            time.sleep(1.5)

            page.fill("#elec_consumption_consumption", cfg.get("conso_kwh", "0"))
            print(f"📊 Consommation saisie : {cfg.get('conso_kwh', '0')} kWh.")
            time.sleep(1.5)

        else:  # --- GAZ ---
            page.wait_for_url(re.compile("compte/gaz"), timeout=30000)
            print("🔥 Mode gaz détecté, remplissage du formulaire...")

            page.wait_for_selector("#gas_consumption_consumptionType", timeout=10000)
            page.select_option("#gas_consumption_consumptionType", value="1")
            print("📈 Mode de saisie : Je connais ma consommation annuelle.")
            page.fill("#gas_consumption_consumption", cfg.get("conso_kwh", "0"))
            print(f"📊 Consommation gaz saisie : {cfg.get('conso_kwh', '0')} kWh.")
            time.sleep(1.5)

        print("➡️ Validation de l’étape 2...")
        page.click("button.btn-next")
        print("✅ Étape 2 validée (consommation).")
        page.wait_for_load_state("networkidle")


        # ⚙️ Étape 3 : Critères de tri avant affichage des offres
        print("⚙️ Étape 3 : Configuration des critères de tri...")

        print("⏳ Attente du chargement du formulaire de tri...")
        for i in range(30):
            try:
                if page.is_visible("#search_configuration_sortModel_sortBy"):
                    print("✅ Formulaire de tri détecté.")
                    break
            except:
                pass
            time.sleep(1)
        else:
            raise Exception("❌ Le formulaire de tri n’est pas apparu après 30s.")

        # --- Sélection tri / ordre / période / comparaison ---
        print("⏳ Sélection du critère de tri 'Coût estimé hors promo/remise'...")
        page.select_option("#search_configuration_sortModel_sortBy", value="2")
        print("✅ Critère de tri sélectionné.")

        print("⏳ Sélection de l’ordre du tri 'Du - cher au + cher'...")
        page.click("label[for='search_configuration_sortModel_sortDirection_0']")
        print("✅ Ordre du tri sélectionné.")

        print("⏳ Sélection de la durée '12 mois'...")
        page.select_option("#search_configuration_period", value="12")
        print("✅ Durée sélectionnée : 12 mois.")

        print("⏳ Réponse à la question 'Comparer avec mon offre actuelle'...")
        page.click("label[for='search_configuration_currentOfferModel_enabled_1']")
        print("✅ Comparaison avec offre actuelle : Non.")

        print("➡️ Validation de l’étape 3 (critères de tri)...")
        for i in range(30):
            try:
                if page.is_visible("button.btn-next"):
                    page.click("button.btn-next")
                    print("✅ Étape 3 validée, chargement des offres...")
                    break
            except:
                pass
            time.sleep(1)
        else:
            raise Exception("❌ Le bouton 'Suivant' n’a pas été trouvé après 30s.")
        
        # --- POPUP résultats ---
        print("⏳ Attente de la popup de confirmation des résultats...")
        for i in range(30):
            try:
                if page.is_visible("button.close-modal"):
                    print("✅ Popup détectée, clic sur 'Ok' pour continuer.")
                    page.click("button.close-modal")
                    break
            except:
                pass
            time.sleep(1)
        else:
            print("⚠️ Aucune popup détectée après 30s (peut déjà être fermée).")

        time.sleep(2)

        # --- Attente des résultats ---
        print("⏳ Attente du chargement des offres...")
        try:
            page.wait_for_selector("div.offre.offer", timeout=60000)
            print("📄 Résultats des offres chargés.")
        except:
            print("⚠️ Résultats non trouvés après 60s.")
            html = page.content()
            with open("debug.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("💾 Page enregistrée dans debug.html pour diagnostic.")
            browser.close()
            return []
        
        # --- Pagination jusqu'à la fin ---
        print("⏳ Chargement de toutes les offres (pagination)...")
        while True:
            try:
                bouton_visible = page.locator("a.btn-prev.btn-next.paginate:not(.hidden)").is_visible()
                if not bouton_visible:
                    print("✅ Toutes les offres sont désormais affichées.")
                    break
                print("➡️ Clic sur 'Offres suivantes' pour charger plus d'offres...")
                page.click("a.btn-prev.btn-next.paginate:not(.hidden)")
                page.wait_for_selector("div.offre.offer", timeout=15000)
                time.sleep(2)
            except Exception as e:
                print(f"⚠️ Fin de pagination (ou erreur mineure détectée) : {e}")
                break

        print("📦 Toutes les offres ont été chargées, passage à l’extraction HTML...")

        # Extraction HTML
        html = page.content()
        browser.close()

    # ====== Analyse des offres (seule partie modifiée) ======
    soup = BeautifulSoup(html, "html.parser")
    offres_html = soup.select("div.offre.offer")
    print(f"🔍 {len(offres_html)} offres détectées")

    data = []
    for bloc in offres_html:
        # Fournisseur via logo alt si possible
        fournisseur_logo = bloc.select_one(".service img[alt]")
        fournisseur = fournisseur_logo["alt"].strip() if fournisseur_logo and fournisseur_logo.has_attr("alt") else ""

        # Nom d'offre
        offre_h4 = bloc.select_one("h4")
        offre_nom = offre_h4.get_text(strip=True) if offre_h4 else ""

        # Total TTC
        total_span = bloc.select_one(".offre-price .total span")
        prix_total_ttc = to_float_fr(total_span.get_text()) if total_span else None

        # Prix total TTC remisé (si présent)
        promo_span = bloc.select_one("span.avec-promotion")
        prix_total_remise = to_float_fr(promo_span.get_text()) if promo_span else None

        # Abonnement TTC
        prix_abonnement = None
        for p in bloc.select("div.prix p, div.prix-offer p"):
            txt = p.get_text(" ", strip=True)
            if "Prix de l'abonnement" in txt:
                strong = p.find("strong")
                val = strong.get_text(" ", strip=True) if strong else txt
                prix_abonnement = to_float_fr(val)
                break

        # Puissance
        puissance = ""
        for p in bloc.select("div.prix p, div.prix-offer p"):
            txt = p.get_text(" ", strip=True)
            if "Puissance" in txt:
                strong = p.find("strong")
                puissance = strong.get_text(strip=True) if strong else txt.split(":", 1)[-1].strip()
                break

        # --- Prix du kWh HP / HC / Base ---
        prix_hp = None
        prix_hc = None

        # On cherche toutes les lignes contenant "kWh" ou "Heures"
        for p in bloc.select("div.prix p, div.prix-offer p"):
            txt = p.get_text(" ", strip=True)
            if not re.search(r"kWh|heure|Heure|HP|HC", txt, re.I):
                continue

            # Cas "Hp : 0,1977€ TTC" ou "HP : 0,1977 € TTC"
            m_hp = re.search(r"H[pP]\s*[:\-]?\s*([0-9\s,\,\.]+)", txt)
            # Cas "Hc : 0,1559€ TTC" ou "HC : 0,1559 € TTC"
            m_hc = re.search(r"H[cC]\s*[:\-]?\s*([0-9\s,\,\.]+)", txt)

            # Cas "Heures Pleines : 0,1977 € TTC"
            m_hp2 = re.search(r"Heures?\s*Pleines?\s*[:\-]?\s*([0-9\s,\,\.]+)", txt, re.I)
            # Cas "Heures Creuses : 0,1559 € TTC"
            m_hc2 = re.search(r"Heures?\s*Creuses?\s*[:\-]?\s*([0-9\s,\,\.]+)", txt, re.I)

            # Cas "kWh Heures Pleines : 0,1977€ TTC"
            m_hp3 = re.search(r"kWh\s*Heures?\s*Pleines?\s*[:\-]?\s*([0-9\s,\,\.]+)", txt, re.I)
            m_hc3 = re.search(r"kWh\s*Heures?\s*Creuses?\s*[:\-]?\s*([0-9\s,\,\.]+)", txt, re.I)

            # Cas générique "Prix du kWh : 0,2 €"
            m_simple = re.search(r"([0-9\s,\.]+)\s*€", txt)

            # Attribution
            if m_hp and not prix_hp:
                prix_hp = to_float_fr(m_hp.group(1))
            if m_hc and not prix_hc:
                prix_hc = to_float_fr(m_hc.group(1))
            if m_hp2 and not prix_hp:
                prix_hp = to_float_fr(m_hp2.group(1))
            if m_hc2 and not prix_hc:
                prix_hc = to_float_fr(m_hc2.group(1))
            if m_hp3 and not prix_hp:
                prix_hp = to_float_fr(m_hp3.group(1))
            if m_hc3 and not prix_hc:
                prix_hc = to_float_fr(m_hc3.group(1))

            # Si un seul prix trouvé → Base
            if prix_hp is None and prix_hc is None and m_simple:
                prix_hp = to_float_fr(m_simple.group(1))

        # Si aucune des variantes n'a fonctionné, on laisse None

        # % vert
        pourcentage_vert_el = bloc.select_one(".label-right")
        pourcentage_vert = pourcentage_vert_el.get_text(strip=True) if pourcentage_vert_el else ""

        # --- Mécanique d’évolution du prix (améliorée pour GAZ et ELEC) ---
        mecanique = ""

        pe_block = bloc.select_one(".price-evolution-block")
        if pe_block:
            # 1️⃣ On cible l'icône principale d'évolution des prix
            icon_el = pe_block.select_one("i.price-evolution-icon")
            if icon_el:
                classes = " ".join(icon_el.get("class", []))
                tooltip_html = icon_el.get("data-original-title", "") or ""
                tooltip_text = BeautifulSoup(tooltip_html, "html.parser").get_text(" ", strip=True).lower()

                # 2️⃣ Détection par classes connues (les plus fiables)
                MAP_CLASS = [
                    (r"icon-tarif-fixe-hors-acheminement", "Prix fixe hors acheminement"),
                    (r"icon-tarif-fixe", "Prix entièrement fixe"),
                    (r"icon-tarif-reglemente", "Tarif réglementé"),
                    (r"icon-tarif-autre", "Autre évolution"),
                    (r"icon-tarif-dynamique", "Offre à tarification dynamique"),
                    (r"icon-tarif-indexe-reference", "Prix indexé sur le prix repère (PRVG)"),
                ]
                for pat, label in MAP_CLASS:
                    if re.search(pat, classes, flags=re.I):
                        mecanique = label
                        break

                # 3️⃣ Si icône générique "indexe", on lit le tooltip
                if not mecanique and re.search(r"icon-tarif-indexe", classes, re.I):
                    if "repère" in tooltip_text or "prvg" in tooltip_text:
                        mecanique = "Prix indexé sur le prix repère (PRVG)"
                    elif "marché" in tooltip_text:
                        mecanique = "Indexé sur le prix de marché"
                    elif "trv" in tooltip_text or "tarif réglementé" in tooltip_text:
                        mecanique = "Prix indexé sur TRV"
                    else:
                        mecanique = "Indexé (autre)"

                # 4️⃣ Fallback : si rien trouvé, on tente le tooltip global
                if not mecanique:
                    for k in [
                        "Prix entièrement fixe",
                        "Prix fixe hors acheminement",
                        "Prix indexé sur TRV",
                        "Prix indexé sur le prix repère",
                        "Indexé sur le prix de marché",
                        "Offre à tarification dynamique",
                        "Tarif réglementé",
                        "Autre évolution",
                    ]:
                        if k.lower() in tooltip_text:
                            mecanique = k
                            break

                # 🎯 Correction spécifique électricité : PRVG → TRV
                is_elec = bloc.select_one("i.icon-electricite") is not None
                if is_elec and mecanique == "Prix indexé sur le prix repère (PRVG)":
                    mecanique = "Prix indexé TRV"

        mecanique = mecanique.strip() if mecanique else "Non renseigné"


        # --- Ajustement : si pas de remise, on copie le prix total ---
        if prix_total_remise is None and prix_total_ttc is not None:
            prix_total_remise = prix_total_ttc

        # Niveau de stabilité (A-D) depuis le tooltip du logo
        stabilite = ""
        stab_i = bloc.select_one("i.offer-tooltip:has(img.price-evolution-certainty-level-logo)")
        if stab_i and stab_i.has_attr("data-original-title"):
            raw = BeautifulSoup(stab_i["data-original-title"], "html.parser").get_text(" ", strip=True)
            m = re.search(r"Niveau de stabilit[ée]\s+du prix\s+([A-D])", raw, flags=re.I)
            if m:
                stabilite = m.group(1)

        # ✅ Formatage lisible de la colonne "Puissance"
        type_energie = cfg.get("type_energie", "").lower()
        if type_energie.startswith("gaz"):
            # Pour le gaz → uniquement la conso
            puissance_affichee = f"{cfg.get('conso_kwh', '')} kWh/an"
        else:
            # Pour l’électricité → Puissance + Profil + Conso
            puissance_val = cfg.get("puissance", "")
            profil_val = cfg.get("profil", "")
            conso_val = cfg.get("conso_kwh", "")
            puissance_affichee = f"{puissance_val} kVA - {profil_val} - {conso_val} kWh/an"

        # ✅ Ajout de la ligne finale
        data.append({
            "Date relevé": datetime.today().strftime("%Y-%m-%d"),
            "Ville": f"{cfg.get('ville', '?')} ({cfg.get('code_postal', '?')})",
            "Type d’énergie": cfg.get("type_energie", ""),
            "Profil": cfg.get("profil", ""),  # évite KeyError pour GAZ
            "Fournisseur": fournisseur,
            "Offre": offre_nom,
            "Puissance": puissance_affichee,  # ✅ plus clair et homogène
            "Prix de l’abonnement TTC": prix_abonnement,
            "Prix HP kWh TTC": prix_hp,
            "Prix HC kWh TTC": prix_hc,
            "Prix total TTC": prix_total_ttc,
            "Prix total TTC remisé": prix_total_remise,
            "% vert": pourcentage_vert,
            "Mécanique": mecanique,
            "Niveau de stabilité": stabilite,
        })

    return data

# --- Lancement de tous les profils ---
all_offres = []
for cfg in CONFIGS:
    try:
        offres = extraire_offres(cfg)
        # on ajoute le profil ID à chaque ligne
        for offre in offres:
            offre["Profil ID"] = cfg["profil_id"]
        all_offres.extend(offres)
    except Exception as e:
        print(f"❌ Erreur pour {cfg['profil_id']} : {e}")
        continue

# --- Export final consolidé ---
if all_offres:
    df = pd.DataFrame(all_offres)

    # Mise au bon format numérique
    for col in [
        "Prix de l’abonnement TTC",
        "Prix HP kWh TTC",
        "Prix HC kWh TTC",
        "Prix total TTC",
        "Prix total TTC remisé",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Positionnement par profil (1 = moins cher sur Prix total TTC) ---
    if "Profil ID" in df.columns and "Prix total TTC" in df.columns:
        # on évite de classer des lignes sans prix total
        df["Positionnement"] = (
            df.groupby("Profil ID")["Prix total TTC"]
              .rank(method="first", ascending=True)
              .astype("Int64")
        )
    else:
        # fallback global si jamais Profil ID manquait
        df["Positionnement"] = df["Prix total TTC"].rank(method="first", ascending=True).astype("Int64")
    # --- Positionnement remisé (1 = moins cher sur Prix total TTC remisé) ---
    if "Profil ID" in df.columns and "Prix total TTC remisé" in df.columns:
        df["Positionnement prix remisé"] = (
            df.groupby("Profil ID")["Prix total TTC remisé"]
              .rank(method="first", ascending=True)
              .astype("Int64")
        )
    else:
        # fallback global
        df["Positionnement prix remisé"] = (
            df["Prix total TTC remisé"].rank(method="first", ascending=True).astype("Int64")
        )

    # (facultatif) tri d'affichage dans le fichier
    df = df.sort_values(["Profil ID", "Positionnement", "Prix total TTC"], ascending=[True, True, True])

    # Envoi BigQuery (Option A : overwrite de la partition du jour)
    write_to_bigquery_overwrite_today(df)
else:
    print("\n⚠️ Aucune offre trouvée.")
