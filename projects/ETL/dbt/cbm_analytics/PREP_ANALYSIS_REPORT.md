# 📊 Rapport d'Analyse PREP - Version Complète

**Généré le** : 2025-12-05 15:38:21

---

## 🎯 Résumé Global

| Métrique | Valeur |
|----------|--------|
| **Tables analysées** | 28 |
| **Lignes totales** | 9,966,684 |
| **Colonnes ODS** | 5,623 |
| **Colonnes PREP** | 1,072 (19.1%) |
| **Colonnes exclues** | 4,551 (80.9%) |

### 📉 Raisons d'Exclusion

- 🔧 **Colonnes techniques ETL** : 166
- ❌ **Colonnes 100% NULL** : 2913
- 📌 **Colonnes constantes** : 1391
- ⚠️ **Colonnes faible valeur** : 81
- 🚫 **Blacklist utilisateur** : 0

---

## 📋 Détail par Table

### 📦 clcondi

| Métrique | Valeur |
|----------|--------|
| Lignes | 429,907 |
| Colonnes ODS | 166 |
| Colonnes PREP | 17 |
| Exclues | 149 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `fam_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | 100% NULL | 100.0% NULL, 0 val. |
| `c_promo` | >95% NULL (99.6%) + 2 valeur(s) distincte(s) | 99.6% NULL, 2 val. |
| `remise3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cou_fix` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `qte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `sf_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `px_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_rem1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_4` | 100% NULL | 100.0% NULL, 0 val. |
| `pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_cli` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `gratuit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `remise4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `no_cond` | 100% NULL | 100.0% NULL, 0 val. |
| `s2_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `s3_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_av_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_av_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_av_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_av_4` | 100% NULL | 100.0% NULL, 0 val. |
| `chx_promo` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_brut` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ope_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_rfa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `exc_pro` | 100% NULL | 100.0% NULL, 0 val. |
| `aut_tar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `eve_exc` | >95% NULL (97.4%) + 3 valeur(s) distincte(s) | 97.4% NULL, 3 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `exc_cli` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_6` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_7` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_8` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_9` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_10` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_trs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `chg_tarif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `qte_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_gra` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `intitule` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_clcondi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 client

| Métrique | Valeur |
|----------|--------|
| Lignes | 43 |
| Colonnes ODS | 321 |
| Colonnes PREP | 47 |
| Exclues | 274 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `k_post` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `form_jur` | 100% NULL | 100.0% NULL, 0 val. |
| `capital` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `statut` | Cardinalité très faible (2 valeurs: [4, 0]) | 0.0% NULL, 2 val. |
| `qui_sta` | Valeur constante (1 seule valeur) | 88.4% NULL, 1 val. |
| `fac_ttc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fac_pxa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fra_app` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `div_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `div_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `div_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `val_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `val_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `val_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_art_1` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_art_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_art_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_art_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_art_5` | 100% NULL | 100.0% NULL, 0 val. |
| `transpor` | Cardinalité très faible (2 valeurs: [0, 173]) | 0.0% NULL, 2 val. |
| `mnt_rlk_1` | 100% NULL | 100.0% NULL, 0 val. |
| `mnt_rlk_2` | 100% NULL | 100.0% NULL, 0 val. |
| `mnt_rlk_3` | 100% NULL | 100.0% NULL, 0 val. |
| `mnt_rlk_4` | 100% NULL | 100.0% NULL, 0 val. |
| `televen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cal_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cal_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `heure` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_tel` | 100% NULL | 100.0% NULL, 0 val. |
| `jou_dec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_livre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_fact` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_paye` | Cardinalité très faible (2 valeurs: [0, 8863]) | 0.0% NULL, 2 val. |
| `cl_stat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `commerc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `app_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `tel_j1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `repert` | 100% NULL | 100.0% NULL, 0 val. |
| `borne` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_ech_d` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gencod` | Cardinalité très faible (2 valeurs: ['3', '12345678987654321']) | 83.7% NULL, 2 val. |
| `cl_plnat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_plreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `maj_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tva_ech` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib` | Valeur constante (1 seule valeur) | 93.0% NULL, 1 val. |
| `rem_glo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_glo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cde_cpl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coeff_pvc` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('100.00')]) | 0.0% NULL, 2 val. |
| `liv_cpl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bi_1` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bi_2` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bi_3` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bi_4` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bi_5` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bs_1` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bs_2` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bs_3` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bs_4` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_bs_5` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_r_1` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_r_2` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_r_3` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_r_4` | 100% NULL | 100.0% NULL, 0 val. |
| `arr_r_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fac_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_factor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `transit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grp_ps` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `blok_enc` | 100% NULL | 100.0% NULL, 0 val. |
| `effectif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ca_client` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ca_pot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `login` | 100% NULL | 100.0% NULL, 0 val. |
| `log_mdp` | 100% NULL | 100.0% NULL, 0 val. |
| `gencod_det` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ord_affec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `priorite` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jalon_tot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_1` | 100% NULL | 100.0% NULL, 0 val. |
| `int_rrr_2` | 100% NULL | 100.0% NULL, 0 val. |
| `int_rrr_3` | 100% NULL | 100.0% NULL, 0 val. |
| `int_rrr_4` | 100% NULL | 100.0% NULL, 0 val. |
| `int_rrr_5` | 100% NULL | 100.0% NULL, 0 val. |
| `int_rrr_6` | 100% NULL | 100.0% NULL, 0 val. |
| `ben_rrr_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ben_rrr_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ben_rrr_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ben_rrr_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ben_rrr_5` | 100% NULL | 100.0% NULL, 0 val. |
| `ben_rrr_6` | 100% NULL | 100.0% NULL, 0 val. |
| `fvte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_adh` | 100% NULL | 100.0% NULL, 0 val. |
| `cadencier` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `interv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_5` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_mini` | Cardinalité très faible (2 valeurs: [0, 500]) | 0.0% NULL, 2 val. |
| `plus_bpbl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bl_regr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_com` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `inc_deb` | Cardinalité très faible (2 valeurs: ['DAP', 'EXW']) | 79.1% NULL, 2 val. |
| `trs_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fr_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_lig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_tlv` | 100% NULL | 100.0% NULL, 0 val. |
| `frq_a_nul` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `frq_v_nul` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mod_a` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_v` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vis_j7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prep_isol` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_v` | 100% NULL | 100.0% NULL, 0 val. |
| `f_verifcde_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_verifcde_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_verifcde_3` | 100% NULL | 100.0% NULL, 0 val. |
| `enc_imp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `plan_rrr` | 100% NULL | 100.0% NULL, 0 val. |
| `szon_geo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `origine` | 100% NULL | 100.0% NULL, 0 val. |
| `s2_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `s3_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_stn_9` | 100% NULL | 100.0% NULL, 0 val. |
| `rq_pan` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_con` | Cardinalité très faible (2 valeurs: ['PCE', 'MM']) | 60.5% NULL, 2 val. |
| `typ_pxa` | 100% NULL | 100.0% NULL, 0 val. |
| `c_retr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_prc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ffc_prc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rfa_soc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `interloc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `interloc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_web` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_prc` | 100% NULL | 100.0% NULL, 0 val. |
| `pref_ref` | 100% NULL | 100.0% NULL, 0 val. |
| `dgc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_tar_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `meth_rf` | 100% NULL | 100.0% NULL, 0 val. |
| `ty_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retour_date` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cat_cum` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_5` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_6` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_5` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_6` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_cad` | 100% NULL | 100.0% NULL, 0 val. |
| `dep_unq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lias_bl` | Cardinalité très faible (2 valeurs: [3, 1]) | 0.0% NULL, 2 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `notation` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ctrl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_floc` | 100% NULL | 100.0% NULL, 0 val. |
| `contrat_date` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `date_px` | 100% NULL | 100.0% NULL, 0 val. |
| `or_ces` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_cam` | 100% NULL | 100.0% NULL, 0 val. |
| `marge_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_veh` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prix_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rlk_mnt` | Valeur constante (1 seule valeur) | 97.7% NULL, 1 val. |
| `no_port` | 100% NULL | 100.0% NULL, 0 val. |
| `maqf_etq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_sup` | 100% NULL | 100.0% NULL, 0 val. |
| `groupe` | Valeur constante (1 seule valeur) | 97.7% NULL, 1 val. |
| `dep_uniq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sscc_sor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `refrec` | 100% NULL | 100.0% NULL, 0 val. |
| `refrec2` | 100% NULL | 100.0% NULL, 0 val. |
| `vc_subst_g` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_ref` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bloq_prep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_plcon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_enc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_aco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mod_aco` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_client` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `jamais_peser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_1` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_app_2` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_app_3` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_app_4` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_app_5` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_app_6` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_app_7` | 100% NULL | 100.0% NULL, 0 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adresse5` | 100% NULL | 100.0% NULL, 0 val. |
| `or_spereg` | 100% NULL | 100.0% NULL, 0 val. |
| `conso_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 crn

| Métrique | Valeur |
|----------|--------|
| Lignes | 836 |
| Colonnes ODS | 61 |
| Colonnes PREP | 7 |
| Exclues | 54 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `ville` | 100% NULL | 100.0% NULL, 0 val. |
| `form_jur` | 100% NULL | 100.0% NULL, 0 val. |
| `capital` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `pays` | 100% NULL | 100.0% NULL, 0 val. |
| `internet` | Valeur constante (1 seule valeur) | 99.9% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `k_post2` | 100% NULL | 100.0% NULL, 0 val. |
| `effectif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ca_crn` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `civilite` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_2` | 100% NULL | 100.0% NULL, 0 val. |
| `num_tel` | 100% NULL | 100.0% NULL, 0 val. |
| `num_fax` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse4` | 100% NULL | 100.0% NULL, 0 val. |
| `no_info` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `texte` | Valeur constante (1 seule valeur) | 99.9% NULL, 1 val. |
| `part` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `famille` | 100% NULL | 100.0% NULL, 0 val. |
| `type` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_crn` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse5` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 crnar

| Métrique | Valeur |
|----------|--------|
| Lignes | 429 |
| Colonnes ODS | 61 |
| Colonnes PREP | 13 |
| Exclues | 48 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `pp_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poid_brut_1` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_brut_2` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_brut_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gencod-v` | Valeur constante (1 seule valeur) | 99.8% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_crnar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_1` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_2` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_univ` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_conv` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_surv` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 deppro

| Métrique | Valeur |
|----------|--------|
| Lignes | 2,803,213 |
| Colonnes ODS | 106 |
| Colonnes PREP | 6 |
| Exclues | 100 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `dat_prin` | 100% NULL | 100.0% NULL, 0 val. |
| `px_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_emp` | 100% NULL | 100.0% NULL, 0 val. |
| `ges_smp` | 100% NULL | 100.0% NULL, 0 val. |
| `achat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_cpt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prod_gp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ndos` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pmp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `d_pxach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_ent` | 100% NULL | 100.0% NULL, 0 val. |
| `px_rvt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_av` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dpx_rvt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `magasin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `emplac` | 100% NULL | 100.0% NULL, 0 val. |
| `mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `maxi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_qua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mini_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mult_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pp_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `classe` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_dep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `del_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `del_mad` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prefixe` | 100% NULL | 100.0% NULL, 0 val. |
| `px_std` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_uniq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_refv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fpx_refv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_fpxv` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_sta` | 100% NULL | 100.0% NULL, 0 val. |
| `ctrl_qua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_for` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_pon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_var_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_var_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_var_3` | 100% NULL | 100.0% NULL, 0 val. |
| `dgc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fpx_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `chg_jour` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `res_qua` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_ctr` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ctr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_sim` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_ttc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_coef` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_div_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_6` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_7` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_8` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_9` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_10` | 100% NULL | 100.0% NULL, 0 val. |
| `f_mv` | 100% NULL | 100.0% NULL, 0 val. |
| `f_uv` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cv` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_perte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vocal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_6` | 100% NULL | 100.0% NULL, 0 val. |
| `des_voc` | 100% NULL | 100.0% NULL, 0 val. |
| `detrompeur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_ade` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_sto` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_sto` | 100% NULL | 100.0% NULL, 0 val. |
| `ges_rot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_deppro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `enc_db` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_nb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `collection` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_remp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_remp` | 100% NULL | 100.0% NULL, 0 val. |
| `stat_remp` | 100% NULL | 100.0% NULL, 0 val. |
| `art_ref` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `stk_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `stk_maxi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `classm` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_sai` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nbsem_na` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nbsem_eca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_turb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 entetcli

| Métrique | Valeur |
|----------|--------|
| Lignes | 12,400 |
| Colonnes ODS | 463 |
| Colonnes PREP | 129 |
| Exclues | 334 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_sai` | Cardinalité très faible (2 valeurs: ['C', 'F']) | 0.0% NULL, 2 val. |
| `typ_cde` | Cardinalité très faible (2 valeurs: [1, 2]) | 0.0% NULL, 2 val. |
| `adr_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fra_app` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adr_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `taux_esc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fac_ttc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `div_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `div_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `div_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `val_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `val_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `val_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cl_livre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_stat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `commerc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `app_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tournee` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_fac` | >95% NULL (98.1%) + 3 valeur(s) distincte(s) | 98.1% NULL, 3 val. |
| `mt_reg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_acom` | Cardinalité très faible (2 valeurs: [Decimal('194.40'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_glo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_glo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `sem_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_plnat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_plreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_reg` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_marg` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_rport` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib` | Valeur constante (1 seule valeur) | 97.3% NULL, 1 val. |
| `nat_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_mar_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_5` | 100% NULL | 100.0% NULL, 0 val. |
| `eta_dev` | 100% NULL | 100.0% NULL, 0 val. |
| `no_cheque` | 100% NULL | 100.0% NULL, 0 val. |
| `no_cb` | 100% NULL | 100.0% NULL, 0 val. |
| `ticket` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_caisse` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bareme` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dev_reg` | 100% NULL | 100.0% NULL, 0 val. |
| `classif` | 100% NULL | 100.0% NULL, 0 val. |
| `affaire` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_ech` | 100% NULL | 100.0% NULL, 0 val. |
| `televen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dr_sit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_factor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retenu` | 100% NULL | 100.0% NULL, 0 val. |
| `caution` | Valeur constante (1 seule valeur) | 84.1% NULL, 1 val. |
| `transit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grp_ps` | 100% NULL | 100.0% NULL, 0 val. |
| `saison` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_ht_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `gencod_det` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rgt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_carte` | 100% NULL | 100.0% NULL, 0 val. |
| `conveuro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_lance` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_dbl` | 100% NULL | 100.0% NULL, 0 val. |
| `tk_anu` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `deb_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `fin_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `col_vis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sem_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sta_dev` | 100% NULL | 100.0% NULL, 0 val. |
| `ter_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cvv2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_tlv` | 100% NULL | 100.0% NULL, 0 val. |
| `no_bpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `contrat` | 100% NULL | 100.0% NULL, 0 val. |
| `ind_con` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_va` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `dat_rep` | 100% NULL | 100.0% NULL, 0 val. |
| `st_interne` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `szon_geo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_dep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_camion` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_rec` | 100% NULL | 100.0% NULL, 0 val. |
| `delai_trs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `datliv_imp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `regr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prep_isol` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_pxa` | Valeur constante (1 seule valeur) | 85.9% NULL, 1 val. |
| `retr_cess` | Cardinalité très faible (2 valeurs: ['C', 'R']) | 83.4% NULL, 2 val. |
| `dep_csg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zone_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `arc_fax` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `id_web` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_3` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_4` | 100% NULL | 100.0% NULL, 0 val. |
| `refus_dev` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_gemet` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_gdest` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_refcde` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_livdem` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dur_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_tar_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vte_fid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_carte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_point` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_reu` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_5` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_6` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_5` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_6` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_6` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_7` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_8` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_9` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_10` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_11` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_12` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_13` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_14` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_15` | 100% NULL | 100.0% NULL, 0 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `no_pre_fact` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_for` | 100% NULL | 100.0% NULL, 0 val. |
| `ind_rev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `location` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_cpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_pas` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `edt_dgd` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_edt` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_rgp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adr_edi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gst_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `marge_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ori_ate` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif_ar` | 100% NULL | 100.0% NULL, 0 val. |
| `af_fin` | >95% NULL (99.8%) + 2 valeur(s) distincte(s) | 99.8% NULL, 2 val. |
| `typ_veh` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_fac_edi` | >95% NULL (98.0%) + 2 valeur(s) distincte(s) | 98.0% NULL, 2 val. |
| `origine` | 100% NULL | 100.0% NULL, 0 val. |
| `prix_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_cv` | 100% NULL | 100.0% NULL, 0 val. |
| `det_frais` | Valeur constante (1 seule valeur) | 38.6% NULL, 1 val. |
| `affret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `m_lin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `no_port` | >95% NULL (98.5%) + 2 valeur(s) distincte(s) | 98.5% NULL, 2 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ordre_m` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tot_marge` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tot_temps` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gc_mvtcon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gc_site` | 100% NULL | 100.0% NULL, 0 val. |
| `gc_incid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gc_ligps` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gc_ligar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gc_ligloc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dur_plor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `aut_retour` | 100% NULL | 100.0% NULL, 0 val. |
| `groupe` | Valeur constante (1 seule valeur) | 99.8% NULL, 1 val. |
| `plus_bpbl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bl_regr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `id_pbxabo` | 100% NULL | 100.0% NULL, 0 val. |
| `id_pbxreq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `contret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_op` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vague` | 100% NULL | 100.0% NULL, 0 val. |
| `cde_cpl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tva_date` | 100% NULL | 100.0% NULL, 0 val. |
| `imm_cam` | 100% NULL | 100.0% NULL, 0 val. |
| `imm_rem` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_cam` | 100% NULL | 100.0% NULL, 0 val. |
| `chauffeur` | 100% NULL | 100.0% NULL, 0 val. |
| `parc_cam` | 100% NULL | 100.0% NULL, 0 val. |
| `cl_plcon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_alloti` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `edi_noblreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_aco` | Cardinalité très faible (2 valeurs: [Decimal('98.98'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `mod_aco` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `mt_aco_ori` | Cardinalité très faible (2 valeurs: [Decimal('194.40'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `lien_aco` | Cardinalité très faible (2 valeurs: [3723849, 0]) | 0.0% NULL, 2 val. |
| `TabPart_entetcli` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_typdoc` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_nomfic` | 100% NULL | 100.0% NULL, 0 val. |
| `enc_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `annonce` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_depfi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ligcai` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cai_rendu` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `corresp_1` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_2` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_3` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_4` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_5` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_6` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_7` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_8` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_9` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_reg_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `num_tel_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `num_fax_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `num_tel_liv` | 100% NULL | 100.0% NULL, 0 val. |
| `num_fax_liv` | 100% NULL | 100.0% NULL, 0 val. |
| `adrfac5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrliv5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrcde5` | 100% NULL | 100.0% NULL, 0 val. |
| `blq_ate` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `or_sms_env` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_mar6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arrondi_pvdev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reg_cod_1` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_2` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_3` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_4` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_5` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_6` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_7` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_8` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_9` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_10` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_11` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_12` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_13` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_14` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_15` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_ao` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_6` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_7` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_8` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_9` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_10` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_11` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_12` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_13` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_14` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_15` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_6` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_7` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_8` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_9` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_10` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_11` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_12` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_13` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_14` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_15` | 100% NULL | 100.0% NULL, 0 val. |
| `lignecaisse` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_titresto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 entetfou

| Métrique | Valeur |
|----------|--------|
| Lignes | 6,506 |
| Colonnes ODS | 270 |
| Colonnes PREP | 77 |
| Exclues | 193 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_sai` | Cardinalité très faible (2 valeurs: ['C', 'P']) | 0.0% NULL, 2 val. |
| `typ_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adr_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `k_postf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `civ_liv` | Cardinalité très faible (2 valeurs: ['Cpy', 'SA']) | 40.0% NULL, 2 val. |
| `adr_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `k_postl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `sem_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `saison` | 100% NULL | 100.0% NULL, 0 val. |
| `conveuro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_for` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `col_vis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_fof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_6` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_1` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_2` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_3` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_4` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_5` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_6` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_1` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_2` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_3` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_4` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_5` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_6` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_6` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_6` | 100% NULL | 100.0% NULL, 0 val. |
| `trs_deb` | Cardinalité très faible (2 valeurs: [3, 0]) | 0.0% NULL, 2 val. |
| `com_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_enl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prepa` | 100% NULL | 100.0% NULL, 0 val. |
| `ach_cen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_cen` | 100% NULL | 100.0% NULL, 0 val. |
| `szon_geo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_ech` | 100% NULL | 100.0% NULL, 0 val. |
| `port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_rport` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flottant` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `affaire` | 100% NULL | 100.0% NULL, 0 val. |
| `retr_cess` | Cardinalité très faible (2 valeurs: ['C', 'R']) | 75.7% NULL, 2 val. |
| `dep_csg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_rel` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_arc` | 100% NULL | 100.0% NULL, 0 val. |
| `txt_arc` | 100% NULL | 100.0% NULL, 0 val. |
| `txt_rel` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_enl` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_elem` | Cardinalité très faible (2 valeurs: ['TRS', 'FOU']) | 0.0% NULL, 2 val. |
| `tournee` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `civ_enl` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_enl_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_enl_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_enl_3` | 100% NULL | 100.0% NULL, 0 val. |
| `villee` | 100% NULL | 100.0% NULL, 0 val. |
| `payse` | 100% NULL | 100.0% NULL, 0 val. |
| `k_post2e` | 100% NULL | 100.0% NULL, 0 val. |
| `adrenl4` | 100% NULL | 100.0% NULL, 0 val. |
| `com_enl_1` | 100% NULL | 100.0% NULL, 0 val. |
| `com_enl_2` | 100% NULL | 100.0% NULL, 0 val. |
| `com_enl_3` | 100% NULL | 100.0% NULL, 0 val. |
| `com_enl_4` | 100% NULL | 100.0% NULL, 0 val. |
| `com_enl_5` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ctbois` | 100% NULL | 100.0% NULL, 0 val. |
| `tx_ech_d` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_liv` | Cardinalité très faible (2 valeurs: [1201, 0]) | 0.0% NULL, 2 val. |
| `gst_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `txt_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `dr_sit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retenu` | 100% NULL | 100.0% NULL, 0 val. |
| `caution` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_cpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_pas` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rgp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rgt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `affret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ctrl_qua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `m_lin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_of` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_lance` | 100% NULL | 100.0% NULL, 0 val. |
| `transfert_st` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `avis_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `rdv_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pal_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `chauff_rec` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_veh` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_entetfou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `af_fin` | 100% NULL | 100.0% NULL, 0 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif_av` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_1` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_2` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_3` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_4` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_5` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_6` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_7` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_8` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_9` | 100% NULL | 100.0% NULL, 0 val. |
| `adrfac5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrliv5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrenl5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fop` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flottant_ctrl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zone_libre_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_6` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_7` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_8` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_9` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_10` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_11` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_12` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_13` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_14` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_15` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_dat_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_mt_fact` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flottant_no_fact` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_nof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_nof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_nof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_nof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_nof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_dtf_1` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_dtf_2` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_dtf_3` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_dtf_4` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_dtf_5` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_mtf_1` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_mtf_2` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_mtf_3` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_mtf_4` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_mtf_5` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 focondi

| Métrique | Valeur |
|----------|--------|
| Lignes | 102,670 |
| Colonnes ODS | 127 |
| Colonnes PREP | 16 |
| Exclues | 111 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `fam_tar` | >95% NULL (98.4%) + 4 valeur(s) distincte(s) | 98.4% NULL, 4 val. |
| `coef_ach_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_ach_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_ach_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_ach_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refa_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refa_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refa_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_refa_4` | 100% NULL | 100.0% NULL, 0 val. |
| `condi_v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `remise1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_5` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_6` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_7` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_8` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_9` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_10` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_gsd` | >95% NULL (99.9%) + 3 valeur(s) distincte(s) | 99.9% NULL, 3 val. |
| `no_cond` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | Valeur constante (1 seule valeur) | 91.0% NULL, 1 val. |
| `typ_rem1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `derog_fou` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_focondi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `gratuit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_gra` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 fournis

| Métrique | Valeur |
|----------|--------|
| Lignes | 36 |
| Colonnes ODS | 221 |
| Colonnes PREP | 29 |
| Exclues | 192 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `k_post` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cde_valo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `verif_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_ach_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_ach_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_ach_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_ach_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `del_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `del_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `del_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `del_liv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_cde_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_cde_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_cde_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_cde_4` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_cde_1` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_cde_2` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_cde_3` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_cde_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_liv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `h_lim_liv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `del_appro_1` | 100% NULL | 100.0% NULL, 0 val. |
| `del_appro_2` | 100% NULL | 100.0% NULL, 0 val. |
| `del_appro_3` | 100% NULL | 100.0% NULL, 0 val. |
| `del_appro_4` | 100% NULL | 100.0% NULL, 0 val. |
| `relik_1` | 100% NULL | 100.0% NULL, 0 val. |
| `relik_2` | 100% NULL | 100.0% NULL, 0 val. |
| `relik_3` | 100% NULL | 100.0% NULL, 0 val. |
| `relik_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ape` | Cardinalité très faible (2 valeurs: ['4532Z', '5829C']) | 94.4% NULL, 2 val. |
| `form_jur` | Cardinalité très faible (2 valeurs: ['SARL', 'SASU']) | 94.4% NULL, 2 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_dvs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mod_liv` | Valeur constante (1 seule valeur) | 88.9% NULL, 1 val. |
| `repert` | 100% NULL | 100.0% NULL, 0 val. |
| `txt_rec` | Valeur constante (1 seule valeur) | 97.2% NULL, 1 val. |
| `gencod` | Valeur constante (1 seule valeur) | 38.9% NULL, 1 val. |
| `transpor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_geo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `famille` | Cardinalité très faible (2 valeurs: ['AR', 'FOU']) | 0.0% NULL, 2 val. |
| `arc` | Valeur constante (1 seule valeur) | 83.3% NULL, 1 val. |
| `login` | 100% NULL | 100.0% NULL, 0 val. |
| `log_mdp` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_for` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `not_codcli` | 100% NULL | 100.0% NULL, 0 val. |
| `jalon` | Cardinalité très faible (2 valeurs: [1, 5]) | 0.0% NULL, 2 val. |
| `enc_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `notation` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `certifie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `trs_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dev_cap` | Valeur constante (1 seule valeur) | 94.4% NULL, 1 val. |
| `fo_stat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jr_app_1` | 100% NULL | 100.0% NULL, 0 val. |
| `jr_app_2` | 100% NULL | 100.0% NULL, 0 val. |
| `jr_app_3` | 100% NULL | 100.0% NULL, 0 val. |
| `jr_app_4` | 100% NULL | 100.0% NULL, 0 val. |
| `jr_app_5` | 100% NULL | 100.0% NULL, 0 val. |
| `jr_app_6` | 100% NULL | 100.0% NULL, 0 val. |
| `jr_app_7` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `clas_fc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `clas_fc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `clas_fc_3` | 100% NULL | 100.0% NULL, 0 val. |
| `szon_geo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `remise1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `poi_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `exo_om` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `exo_da` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `interloc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `interloc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_web` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_rem1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `pattern_url` | 100% NULL | 100.0% NULL, 0 val. |
| `date_px` | 100% NULL | 100.0% NULL, 0 val. |
| `tx_ech_d` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fo_plnat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fo_plreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fo_plexp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_con` | Cardinalité très faible (2 valeurs: ['PCE', 'MM']) | 47.2% NULL, 2 val. |
| `typ_cam` | 100% NULL | 100.0% NULL, 0 val. |
| `fact_con` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_frapp` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_4` | 100% NULL | 100.0% NULL, 0 val. |
| `nda` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nbdeci` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `deci_fourn` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_golda` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nmok_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grille_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `controleur` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `refrec` | 100% NULL | 100.0% NULL, 0 val. |
| `refrec2` | 100% NULL | 100.0% NULL, 0 val. |
| `bloq_prep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fl_qualite1` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite2` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite3` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite4` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_fournis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adresse5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fop` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rgpd_st` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rgpd_com` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 hisprixa

| Métrique | Valeur |
|----------|--------|
| Lignes | 114,751 |
| Colonnes ODS | 53 |
| Colonnes PREP | 8 |
| Exclues | 45 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `qte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_5` | 100% NULL | 100.0% NULL, 0 val. |
| `depot` | Cardinalité très faible (2 valeurs: [11, 0]) | 0.0% NULL, 2 val. |
| `qte_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ach_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_hisprixa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 histoent

| Métrique | Valeur |
|----------|--------|
| Lignes | 19,014 |
| Colonnes ODS | 455 |
| Colonnes PREP | 118 |
| Exclues | 337 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_mvt` | Cardinalité très faible (2 valeurs: ['C', 'F']) | 0.0% NULL, 2 val. |
| `typ_cde` | Cardinalité très faible (2 valeurs: [1, 2]) | 0.0% NULL, 2 val. |
| `adr_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `k_postf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adr_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ech` | Cardinalité très faible (2 valeurs: [0, 2]) | 0.0% NULL, 2 val. |
| `taux_esc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_livre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tarif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_stat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `commerc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `app_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `borne` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tournee` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_reg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_acom` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `maj_ach` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('6.00')]) | 0.0% NULL, 2 val. |
| `rem_glo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_glo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cl_plnat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_plreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_rport` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib` | Valeur constante (1 seule valeur) | 97.5% NULL, 1 val. |
| `nat_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_marg` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_fof_6` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_1` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_2` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_3` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_4` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_5` | 100% NULL | 100.0% NULL, 0 val. |
| `frais_6` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_1` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_2` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_3` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_4` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_5` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_frais_6` | 100% NULL | 100.0% NULL, 0 val. |
| `no_cheque` | 100% NULL | 100.0% NULL, 0 val. |
| `no_cb` | 100% NULL | 100.0% NULL, 0 val. |
| `ticket` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_caisse` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_reg` | 100% NULL | 100.0% NULL, 0 val. |
| `dev_reg` | Cardinalité très faible (2 valeurs: ['N', 'n']) | 76.3% NULL, 2 val. |
| `affaire` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_ech` | 100% NULL | 100.0% NULL, 0 val. |
| `televen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_factor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nofaca_1` | 100% NULL | 100.0% NULL, 0 val. |
| `nofaca_2` | 100% NULL | 100.0% NULL, 0 val. |
| `nofaca_3` | 100% NULL | 100.0% NULL, 0 val. |
| `dafaca_1` | 100% NULL | 100.0% NULL, 0 val. |
| `dafaca_2` | 100% NULL | 100.0% NULL, 0 val. |
| `dafaca_3` | 100% NULL | 100.0% NULL, 0 val. |
| `mtfaca_1` | 100% NULL | 100.0% NULL, 0 val. |
| `mtfaca_2` | 100% NULL | 100.0% NULL, 0 val. |
| `mtfaca_3` | 100% NULL | 100.0% NULL, 0 val. |
| `grp_ps` | 100% NULL | 100.0% NULL, 0 val. |
| `saison` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qui_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_carte` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fa_a_1` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fa_a_2` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fa_a_3` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fa_a_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_lance` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prof_6` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_1` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_2` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_3` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_4` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_5` | 100% NULL | 100.0% NULL, 0 val. |
| `nom_prof_6` | 100% NULL | 100.0% NULL, 0 val. |
| `deb_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `fin_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `col_vis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `cvv2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_tlv` | 100% NULL | 100.0% NULL, 0 val. |
| `contrat` | 100% NULL | 100.0% NULL, 0 val. |
| `ind_con` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_rep` | 100% NULL | 100.0% NULL, 0 val. |
| `szon_geo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_rec` | 100% NULL | 100.0% NULL, 0 val. |
| `delai_trs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `datliv_imp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retr_cess` | Cardinalité très faible (2 valeurs: ['C', 'R']) | 76.1% NULL, 2 val. |
| `dos_pxr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_csg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `temps` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_enl` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_3` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_cde_4` | 100% NULL | 100.0% NULL, 0 val. |
| `dur_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_tar_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vte_fid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_carte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_point` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `usr_not` | 100% NULL | 100.0% NULL, 0 val. |
| `note` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_5` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fil_6` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_5` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cum_6` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_6` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_7` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_8` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_9` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_10` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_11` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_12` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_13` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_14` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_reg_15` | 100% NULL | 100.0% NULL, 0 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_for` | 100% NULL | 100.0% NULL, 0 val. |
| `ind_rev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `location` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mod_edt` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_edi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `txt_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `marge_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ori_ate` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ind_act` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `af_fin` | >95% NULL (99.3%) + 2 valeur(s) distincte(s) | 99.3% NULL, 2 val. |
| `typ_veh` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `origine` | 100% NULL | 100.0% NULL, 0 val. |
| `prix_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_cv` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_rg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `affret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retenu` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_rgt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_rgp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `det_frais` | Valeur constante (1 seule valeur) | 58.3% NULL, 1 val. |
| `no_ctrl_qua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `m_lin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `memory_decimal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `memory_decimal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `memory_decimal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `memory_decimal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `memory_decimal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `s3_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `tot_marge` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tot_temps` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ordre_m` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gc_mvtcon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zone_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `aut_retour` | 100% NULL | 100.0% NULL, 0 val. |
| `groupe` | Valeur constante (1 seule valeur) | 99.9% NULL, 1 val. |
| `com_liv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `com_liv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `id_pbxabo` | 100% NULL | 100.0% NULL, 0 val. |
| `id_pbxreq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_op` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tva_date` | 100% NULL | 100.0% NULL, 0 val. |
| `cl_plcon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_fi2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tar_cu2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_aco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mod_aco` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_aco_ori` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_aco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_histoent` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_alloti` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `edi_noblreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `edi_typdoc` | 100% NULL | 100.0% NULL, 0 val. |
| `edi_nomfic` | 100% NULL | 100.0% NULL, 0 val. |
| `enc_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adr_enl_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_enl_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_enl_3` | 100% NULL | 100.0% NULL, 0 val. |
| `villee` | 100% NULL | 100.0% NULL, 0 val. |
| `k_post2e` | 100% NULL | 100.0% NULL, 0 val. |
| `adrenl4` | 100% NULL | 100.0% NULL, 0 val. |
| `k_poste` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `of_frais` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ligcai` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `edi_gemet` | 100% NULL | 100.0% NULL, 0 val. |
| `cai_rendu` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `edi_gdest` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_1` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_2` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_3` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_4` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_5` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_6` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_7` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_8` | 100% NULL | 100.0% NULL, 0 val. |
| `corresp_9` | 100% NULL | 100.0% NULL, 0 val. |
| `num_tel_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `num_fax_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `num_tel_liv` | 100% NULL | 100.0% NULL, 0 val. |
| `num_fax_liv` | 100% NULL | 100.0% NULL, 0 val. |
| `adrfac5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrliv5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrenl5` | 100% NULL | 100.0% NULL, 0 val. |
| `adrcde5` | 100% NULL | 100.0% NULL, 0 val. |
| `or_sms_env` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_fop` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_mar6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arrondi_pvdev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reg_cod_1` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_2` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_3` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_4` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_5` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_6` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_7` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_8` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_9` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_10` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_11` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_12` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_13` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_14` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cod_15` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_6` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_7` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_8` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_9` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_10` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_11` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_12` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_13` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_14` | 100% NULL | 100.0% NULL, 0 val. |
| `rendu_15` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_6` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_7` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_8` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_9` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_10` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_11` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_12` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_13` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_14` | 100% NULL | 100.0% NULL, 0 val. |
| `trop_percu_15` | 100% NULL | 100.0% NULL, 0 val. |
| `lignecaisse` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_titresto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flottant_ctrl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flottant_val` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 histolig

| Métrique | Valeur |
|----------|--------|
| Lignes | 10,828 |
| Colonnes ODS | 447 |
| Colonnes PREP | 133 |
| Exclues | 314 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | 100% NULL | 100.0% NULL, 0 val. |
| `es_mvt` | Cardinalité très faible (2 valeurs: ['E', 'S']) | 0.0% NULL, 2 val. |
| `remise2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_vte` | Cardinalité très faible (2 valeurs: ['C', 'U']) | 41.7% NULL, 2 val. |
| `lib_surv` | Valeur constante (1 seule valeur) | 99.9% NULL, 1 val. |
| `nb_univ` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_ach` | Cardinalité très faible (2 valeurs: ['C', 'U']) | 49.3% NULL, 2 val. |
| `lib_sura` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_unia` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cona` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_pan` | Valeur constante (1 seule valeur) | 87.9% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `app_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `formule` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niv_pha_1` | 100% NULL | 100.0% NULL, 0 val. |
| `niv_pha_2` | 100% NULL | 100.0% NULL, 0 val. |
| `niv_pha_3` | 100% NULL | 100.0% NULL, 0 val. |
| `nmc_lie` | >95% NULL (98.4%) + 3 valeur(s) distincte(s) | 98.4% NULL, 3 val. |
| `lien_imm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_lot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_dec` | Cardinalité très faible (2 valeurs: [0, 2]) | 0.0% NULL, 2 val. |
| `heure` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cout_trs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `longueur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `largeur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hauteur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_unic` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_marg` | Cardinalité très faible (2 valeurs: ['V', 'F']) | 72.6% NULL, 2 val. |
| `coef_mar_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_5` | 100% NULL | 100.0% NULL, 0 val. |
| `c_promo` | >95% NULL (96.0%) + 2 valeur(s) distincte(s) | 96.0% NULL, 2 val. |
| `poi_var` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `remise3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ref_con` | 100% NULL | 100.0% NULL, 0 val. |
| `affaire` | 100% NULL | 100.0% NULL, 0 val. |
| `fac_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_ori` | Cardinalité très faible (2 valeurs: [435325, 0]) | 0.0% NULL, 2 val. |
| `pret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fin_pret` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_ret` | 100% NULL | 100.0% NULL, 0 val. |
| `ret_pret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fin_gar` | 100% NULL | 100.0% NULL, 0 val. |
| `demande` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rlf` | Cardinalité très faible (2 valeurs: [Decimal('2.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `mt_rlf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `non_rfa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_com` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conveuro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_of` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_reafab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_reacons` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `k_var` | 100% NULL | 100.0% NULL, 0 val. |
| `no_lance` | 100% NULL | 100.0% NULL, 0 val. |
| `nofaca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `md5` | 100% NULL | 100.0% NULL, 0 val. |
| `prod_gp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `location` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `deb_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `fin_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `no_info` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif_rebu` | 100% NULL | 100.0% NULL, 0 val. |
| `col_vis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_serie` | 100% NULL | 100.0% NULL, 0 val. |
| `cde_ori` | 100% NULL | 100.0% NULL, 0 val. |
| `conv_pa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `k_opt` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_grc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_gar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_ctr` | 100% NULL | 100.0% NULL, 0 val. |
| `nat_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `contrat` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_prodad` | 100% NULL | 100.0% NULL, 0 val. |
| `cpx_prodad` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_com` | 100% NULL | 100.0% NULL, 0 val. |
| `crit_cli_df` | Cardinalité très faible (2 valeurs: [3, 0]) | 0.0% NULL, 2 val. |
| `nb_ca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pp_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `regr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retr_cess` | 100% NULL | 100.0% NULL, 0 val. |
| `retr_gere` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_acc` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_cc` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_cc` | 100% NULL | 100.0% NULL, 0 val. |
| `tare` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fret` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `stk_csg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `list_crit` | Cardinalité très faible (2 valeurs: ['1', 'X']) | 44.6% NULL, 2 val. |
| `num_lot` | 100% NULL | 100.0% NULL, 0 val. |
| `num_colis` | 100% NULL | 100.0% NULL, 0 val. |
| `no_mvtbois` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_mvtobois` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_opebois` | 100% NULL | 100.0% NULL, 0 val. |
| `mag_cli` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mag_gen` | 100% NULL | 100.0% NULL, 0 val. |
| `etat` | 100% NULL | 100.0% NULL, 0 val. |
| `inc_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_4` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_point` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ope_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `asso_lig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `eve_exc` | 100% NULL | 100.0% NULL, 0 val. |
| `sous_trait` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_cnv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_mes` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_mtach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_rvt_bo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ns_indrev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `num_prix` | 100% NULL | 100.0% NULL, 0 val. |
| `rendement` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `duree` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_perte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ligct` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_grp_c` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_ach_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_rvt_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_ach_r` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_rvt_r` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_fac` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ctbois` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_ree` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_trs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_cona` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cpt_acc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_sscc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_batch` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ordre_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `derog_fou` | 100% NULL | 100.0% NULL, 0 val. |
| `ref_lig_bud` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_ua1` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `lib_ua2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_ua3` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv1` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ua1` | Cardinalité très faible (2 valeurs: [Decimal('2.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `nb_ua2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_ca3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_uv1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_uv2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cv3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_op` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `crit_param` | 100% NULL | 100.0% NULL, 0 val. |
| `crit_val` | 100% NULL | 100.0% NULL, 0 val. |
| `sous_total` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_u_perte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `df_tax_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_5` | 100% NULL | 100.0% NULL, 0 val. |
| `motif_rem` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_valor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vc_numlig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vc_rang` | 100% NULL | 100.0% NULL, 0 val. |
| `rei_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nai_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dci_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ret_stk` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vpm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `noedit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tickbal` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_bar` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_pv` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_pa` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_por` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_port` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_frais` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_rvt_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_fou_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_prolie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s4_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `marquefl` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_rvt_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_rvt_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_up` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nom_pr_web` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_up` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lissch` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_pv` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_histolig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `valodffl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prioritefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `colis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pal_sol` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_dffl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `apeser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ssfraisfl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_pal` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_col` | 100% NULL | 100.0% NULL, 0 val. |
| `frai_prod` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_pa` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_facfo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ligsce` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sup_cons` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cond_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `intitule` | 100% NULL | 100.0% NULL, 0 val. |
| `arrondi_pvdev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_titresto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 lignecli

| Métrique | Valeur |
|----------|--------|
| Lignes | 39,933 |
| Colonnes ODS | 419 |
| Colonnes PREP | 104 |
| Exclues | 315 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_sai` | Cardinalité très faible (2 valeurs: ['C', 'F']) | 0.0% NULL, 2 val. |
| `remise2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_vte` | Cardinalité très faible (2 valeurs: ['C', 'U']) | 35.3% NULL, 2 val. |
| `lib_surv` | Valeur constante (1 seule valeur) | 68.9% NULL, 1 val. |
| `nb_univ` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_px` | Cardinalité très faible (2 valeurs: [97531, 0]) | 0.0% NULL, 2 val. |
| `fam_pan` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `typ_cdef` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_1` | 100% NULL | 100.0% NULL, 0 val. |
| `commerc_2` | 100% NULL | 100.0% NULL, 0 val. |
| `app_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `total` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `formule` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dlc` | 100% NULL | 100.0% NULL, 0 val. |
| `niv_pha_1` | 100% NULL | 100.0% NULL, 0 val. |
| `niv_pha_2` | 100% NULL | 100.0% NULL, 0 val. |
| `niv_pha_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_pha` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_marg` | 100% NULL | 100.0% NULL, 0 val. |
| `nmc_lie` | >95% NULL (98.5%) + 3 valeur(s) distincte(s) | 98.5% NULL, 3 val. |
| `lien_ims` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_los` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `immatr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `suiv_lot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_mar_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_mar_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_opt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_var` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_promo` | 100% NULL | 100.0% NULL, 0 val. |
| `poi_var` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `remise3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `facture` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_fact` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ref_con` | 100% NULL | 100.0% NULL, 0 val. |
| `master` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fac_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_fac` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fin_gar` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_bp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `num_ave` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_rfa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_com` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fichier` | 100% NULL | 100.0% NULL, 0 val. |
| `k_var` | 100% NULL | 100.0% NULL, 0 val. |
| `md5` | 100% NULL | 100.0% NULL, 0 val. |
| `prod_gp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_of` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_lance` | 100% NULL | 100.0% NULL, 0 val. |
| `statut` | 100% NULL | 100.0% NULL, 0 val. |
| `pdp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pdp_lig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mode_gp` | 100% NULL | 100.0% NULL, 0 val. |
| `nofaca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `location` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `deb_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `fin_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `no_info` | Cardinalité très faible (2 valeurs: [0, 1127]) | 0.0% NULL, 2 val. |
| `col_vis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sem_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_serie` | 100% NULL | 100.0% NULL, 0 val. |
| `k_opt` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_gar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dem_fou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `val_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_bpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rupt_bpr` | 100% NULL | 100.0% NULL, 0 val. |
| `rupt_bpe` | >95% NULL (100.0%) + 2 valeur(s) distincte(s) | 100.0% NULL, 2 val. |
| `contrat` | 100% NULL | 100.0% NULL, 0 val. |
| `ind_con` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lig_con` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s4_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `prio_prod` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_com` | 100% NULL | 100.0% NULL, 0 val. |
| `crit_cli_df` | Cardinalité très faible (2 valeurs: [3, 0]) | 0.0% NULL, 2 val. |
| `nb_cv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pp_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ligsce` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_unic` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_uc` | Cardinalité très faible (2 valeurs: [Decimal('2.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `grille_pan` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tare` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mag_cli` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mag_gen` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_frais` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_mvtobois` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `num_lot` | 100% NULL | 100.0% NULL, 0 val. |
| `num_colis` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ctbois` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ligct` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rem_val_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_4` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_lig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_point` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ope_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `eve_exc` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `no_tar_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `affaire` | 100% NULL | 100.0% NULL, 0 val. |
| `ns_indrev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_trsc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `num_prix` | 100% NULL | 100.0% NULL, 0 val. |
| `rendement` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `duree` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_perte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `blocage` | >95% NULL (99.8%) + 3 valeur(s) distincte(s) | 99.8% NULL, 3 val. |
| `no_grp_c` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_ach_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_rvt_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_ach_r` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_rvt_r` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_fac` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_ppm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_ave` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `etud_nbor` | 100% NULL | 100.0% NULL, 0 val. |
| `etud_qteo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_mes` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_ree` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_trs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `st_interne` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nombre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_mo` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_acc` | 100% NULL | 100.0% NULL, 0 val. |
| `det_frais` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_acc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_sscc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `for_qte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ordre_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grille_rem` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv1` | >95% NULL (100.0%) + 3 valeur(s) distincte(s) | 100.0% NULL, 3 val. |
| `lib_uv2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_uv2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cv3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_fic` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `crit_param` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_op` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `crit_val` | 100% NULL | 100.0% NULL, 0 val. |
| `sous_total` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_u_perte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `df_tax_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_5` | 100% NULL | 100.0% NULL, 0 val. |
| `motif_rem` | 100% NULL | 100.0% NULL, 0 val. |
| `vc_numlig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vc_rang` | 100% NULL | 100.0% NULL, 0 val. |
| `preval` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_valor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rei_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nai_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dci_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vpm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `noedit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tickbal` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_pv` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_bar` | 100% NULL | 100.0% NULL, 0 val. |
| `com_port` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_por` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_rvt_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_frais` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_fou_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_prolie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s4_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `marquefl` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_rvt_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_rvt_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_up` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nom_pr_web` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_up` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `lissch` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_pv` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_lignecli` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `colis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pal_sol` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `valodffl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prioritefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_dffl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `apeser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_forfate` | 100% NULL | 100.0% NULL, 0 val. |
| `ssfraisfl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif_ar` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_pal` | 100% NULL | 100.0% NULL, 0 val. |
| `sta_dev` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_col` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_nt_prep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poid_bt_prep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_por` | 100% NULL | 100.0% NULL, 0 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ord_bp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_mvt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lig_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sup_cons` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `longueur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `largeur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hauteur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_mar6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `intitule` | 100% NULL | 100.0% NULL, 0 val. |
| `arrondi_pvdev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_titresto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 lignefou

| Métrique | Valeur |
|----------|--------|
| Lignes | 18,249 |
| Colonnes ODS | 348 |
| Colonnes PREP | 78 |
| Exclues | 270 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_sai` | Cardinalité très faible (2 valeurs: ['C', 'P']) | 0.0% NULL, 2 val. |
| `uni_ach` | Cardinalité très faible (2 valeurs: ['C', 'U']) | 0.5% NULL, 2 val. |
| `lib_sura` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_unia` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cona` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | 100% NULL | 100.0% NULL, 0 val. |
| `conv_sto` | Cardinalité très faible (2 valeurs: [Decimal('1.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `lien_ime` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_loe` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `immatr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `suiv_lot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_md` | Cardinalité très faible (2 valeurs: ['M', 'D']) | 0.0% NULL, 2 val. |
| `conv_dec` | Cardinalité très faible (2 valeurs: [0, 2]) | 0.0% NULL, 2 val. |
| `remise2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `n_surlig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_unic` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `qte_uc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_promo` | Cardinalité très faible (2 valeurs: ['RA', 'PN']) | 87.9% NULL, 2 val. |
| `remise3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poi_var` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `affaire` | 100% NULL | 100.0% NULL, 0 val. |
| `demande` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `nmc_lie` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `k_var` | 100% NULL | 100.0% NULL, 0 val. |
| `md5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_ouv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_nom` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `col_vis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_pa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_deb` | Cardinalité très faible (2 valeurs: [11, 0]) | 0.0% NULL, 2 val. |
| `nat_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_contrat` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_liv` | Cardinalité très faible (2 valeurs: [1201, 0]) | 0.0% NULL, 2 val. |
| `prepa` | 100% NULL | 100.0% NULL, 0 val. |
| `no_of` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ori_of` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_emp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_consm_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_6` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_7` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_8` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_9` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_10` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_11` | 100% NULL | 100.0% NULL, 0 val. |
| `er_consm_12` | 100% NULL | 100.0% NULL, 0 val. |
| `es_const` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `es_smn` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_smx` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_6` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_7` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_8` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_9` | 100% NULL | 100.0% NULL, 0 val. |
| `er_lib_10` | 100% NULL | 100.0% NULL, 0 val. |
| `lig_cen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_icon_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_6` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_7` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_8` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_9` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_10` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_11` | 100% NULL | 100.0% NULL, 0 val. |
| `er_icon_12` | 100% NULL | 100.0% NULL, 0 val. |
| `er_tex_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_tex_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_tex_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_tex_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_tex_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_sems_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_sems_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_sems_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_sems_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_sems_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_sems_6` | 100% NULL | 100.0% NULL, 0 val. |
| `er_isem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_isem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_isem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_isem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_isem_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_isem_6` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `no_info` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `div_fac` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_surv` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pp_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ligsce` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tare` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `stk_csg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `num_lot` | 100% NULL | 100.0% NULL, 0 val. |
| `num_colis` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ctbois` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_val_4` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ligct` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_lig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_ori` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_cnv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_mes` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vol_mtach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `motif` | 100% NULL | 100.0% NULL, 0 val. |
| `er_objv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_grp_f` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_ach_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_rvt_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_ach_r` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_rvt_r` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mtp_fac` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_grp_tf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cond_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cond_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `list_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `mag_gen` | 100% NULL | 100.0% NULL, 0 val. |
| `derog_fou` | 100% NULL | 100.0% NULL, 0 val. |
| `ref_lig_bud` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_ua1` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `lib_ua2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_ua3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ua1` | Cardinalité très faible (2 valeurs: [Decimal('2.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `nb_ua2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_ca3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_coef_tend` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `k_opt` | 100% NULL | 100.0% NULL, 0 val. |
| `noedit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_pa` | 100% NULL | 100.0% NULL, 0 val. |
| `mt_frais` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_fou_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_prolie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s4_famillefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `marquefl` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_rvt_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_up` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nom_pr_web` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_up` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `lissch` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_lignefou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `valodffl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prioritefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rei_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nai_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `colis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pal_sol` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `fac_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_pa` | 100% NULL | 100.0% NULL, 0 val. |
| `apeser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ssfraisfl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_pal` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_col` | 100% NULL | 100.0% NULL, 0 val. |
| `niveau6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `niveau8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poid_net_a_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poid_brut_a_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_gra` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `dat_enl` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_enl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lien_sscc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `intitule` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_ctrl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flottant_qte_rlf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zone_libre_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_6` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_7` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_8` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_9` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_10` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_11` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_12` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_13` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_14` | 100% NULL | 100.0% NULL, 0 val. |
| `zone_libre_15` | 100% NULL | 100.0% NULL, 0 val. |
| `flottant_no_fact` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 lisval

| Métrique | Valeur |
|----------|--------|
| Lignes | 0 |
| Colonnes ODS | 85 |
| Colonnes PREP | 78 |
| Exclues | 7 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 lisval_fou_production

| Métrique | Valeur |
|----------|--------|
| Lignes | 5 |
| Colonnes ODS | 80 |
| Colonnes PREP | 4 |
| Exclues | 76 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_fich` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `liste` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `za0` | 100% NULL | 100.0% NULL, 0 val. |
| `zn0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd0` | 100% NULL | 100.0% NULL, 0 val. |
| `zc0` | 100% NULL | 100.0% NULL, 0 val. |
| `za1` | 100% NULL | 100.0% NULL, 0 val. |
| `zn1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd1` | 100% NULL | 100.0% NULL, 0 val. |
| `zt1` | 100% NULL | 100.0% NULL, 0 val. |
| `za2` | 100% NULL | 100.0% NULL, 0 val. |
| `zn2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd2` | 100% NULL | 100.0% NULL, 0 val. |
| `zt2` | 100% NULL | 100.0% NULL, 0 val. |
| `za3` | 100% NULL | 100.0% NULL, 0 val. |
| `zn3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd3` | 100% NULL | 100.0% NULL, 0 val. |
| `zt3` | 100% NULL | 100.0% NULL, 0 val. |
| `za4` | 100% NULL | 100.0% NULL, 0 val. |
| `zn4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd4` | 100% NULL | 100.0% NULL, 0 val. |
| `zt4` | 100% NULL | 100.0% NULL, 0 val. |
| `za5` | 100% NULL | 100.0% NULL, 0 val. |
| `zn5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd5` | 100% NULL | 100.0% NULL, 0 val. |
| `zt5` | 100% NULL | 100.0% NULL, 0 val. |
| `za6` | 100% NULL | 100.0% NULL, 0 val. |
| `zn6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd6` | 100% NULL | 100.0% NULL, 0 val. |
| `zt6` | 100% NULL | 100.0% NULL, 0 val. |
| `za7` | 100% NULL | 100.0% NULL, 0 val. |
| `zn7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd7` | 100% NULL | 100.0% NULL, 0 val. |
| `zt7` | 100% NULL | 100.0% NULL, 0 val. |
| `za8` | 100% NULL | 100.0% NULL, 0 val. |
| `zn8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd8` | 100% NULL | 100.0% NULL, 0 val. |
| `zt8` | 100% NULL | 100.0% NULL, 0 val. |
| `za9` | 100% NULL | 100.0% NULL, 0 val. |
| `zn9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd9` | 100% NULL | 100.0% NULL, 0 val. |
| `zt9` | 100% NULL | 100.0% NULL, 0 val. |
| `ze0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_courante` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_autre` | 100% NULL | 100.0% NULL, 0 val. |
| `index_c` | 100% NULL | 100.0% NULL, 0 val. |
| `index_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_l` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_d` | 100% NULL | 100.0% NULL, 0 val. |
| `index_t` | 100% NULL | 100.0% NULL, 0 val. |
| `index_e` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_lisval` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |


### 📦 lisval_produits_lies

| Métrique | Valeur |
|----------|--------|
| Lignes | 4,452 |
| Colonnes ODS | 80 |
| Colonnes PREP | 5 |
| Exclues | 75 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_fich` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `liste` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `za0` | 100% NULL | 100.0% NULL, 0 val. |
| `zn0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd0` | 100% NULL | 100.0% NULL, 0 val. |
| `zc0` | 100% NULL | 100.0% NULL, 0 val. |
| `za1` | 100% NULL | 100.0% NULL, 0 val. |
| `zn1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd1` | 100% NULL | 100.0% NULL, 0 val. |
| `zt1` | 100% NULL | 100.0% NULL, 0 val. |
| `za2` | 100% NULL | 100.0% NULL, 0 val. |
| `zn2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd2` | 100% NULL | 100.0% NULL, 0 val. |
| `zt2` | 100% NULL | 100.0% NULL, 0 val. |
| `za3` | 100% NULL | 100.0% NULL, 0 val. |
| `zn3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd3` | 100% NULL | 100.0% NULL, 0 val. |
| `zt3` | 100% NULL | 100.0% NULL, 0 val. |
| `za4` | 100% NULL | 100.0% NULL, 0 val. |
| `zn4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd4` | 100% NULL | 100.0% NULL, 0 val. |
| `zt4` | 100% NULL | 100.0% NULL, 0 val. |
| `za5` | 100% NULL | 100.0% NULL, 0 val. |
| `zn5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd5` | 100% NULL | 100.0% NULL, 0 val. |
| `zt5` | 100% NULL | 100.0% NULL, 0 val. |
| `za6` | 100% NULL | 100.0% NULL, 0 val. |
| `zn6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd6` | 100% NULL | 100.0% NULL, 0 val. |
| `zt6` | 100% NULL | 100.0% NULL, 0 val. |
| `za7` | 100% NULL | 100.0% NULL, 0 val. |
| `zn7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd7` | 100% NULL | 100.0% NULL, 0 val. |
| `zt7` | 100% NULL | 100.0% NULL, 0 val. |
| `za8` | 100% NULL | 100.0% NULL, 0 val. |
| `zn8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd8` | 100% NULL | 100.0% NULL, 0 val. |
| `zt8` | 100% NULL | 100.0% NULL, 0 val. |
| `za9` | 100% NULL | 100.0% NULL, 0 val. |
| `zn9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd9` | 100% NULL | 100.0% NULL, 0 val. |
| `zt9` | 100% NULL | 100.0% NULL, 0 val. |
| `ze1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_courante` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_autre` | 100% NULL | 100.0% NULL, 0 val. |
| `index_c` | 100% NULL | 100.0% NULL, 0 val. |
| `index_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_l` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_d` | 100% NULL | 100.0% NULL, 0 val. |
| `index_t` | 100% NULL | 100.0% NULL, 0 val. |
| `index_e` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_lisval` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |


### 📦 lisval_produits_vehicules

| Métrique | Valeur |
|----------|--------|
| Lignes | 180,609 |
| Colonnes ODS | 80 |
| Colonnes PREP | 7 |
| Exclues | 73 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_fich` | Cardinalité très faible (2 valeurs: ['p', 'P']) | 0.0% NULL, 2 val. |
| `liste` | Cardinalité très faible (2 valeurs: ['vehic', 'VEHIC']) | 0.0% NULL, 2 val. |
| `za0` | 100% NULL | 100.0% NULL, 0 val. |
| `zn0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zc0` | 100% NULL | 100.0% NULL, 0 val. |
| `za1` | 100% NULL | 100.0% NULL, 0 val. |
| `zn1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd1` | 100% NULL | 100.0% NULL, 0 val. |
| `za2` | 100% NULL | 100.0% NULL, 0 val. |
| `zn2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd2` | 100% NULL | 100.0% NULL, 0 val. |
| `zt2` | 100% NULL | 100.0% NULL, 0 val. |
| `za3` | 100% NULL | 100.0% NULL, 0 val. |
| `zn3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd3` | 100% NULL | 100.0% NULL, 0 val. |
| `zt3` | 100% NULL | 100.0% NULL, 0 val. |
| `za4` | 100% NULL | 100.0% NULL, 0 val. |
| `zn4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd4` | 100% NULL | 100.0% NULL, 0 val. |
| `zt4` | 100% NULL | 100.0% NULL, 0 val. |
| `za5` | 100% NULL | 100.0% NULL, 0 val. |
| `zn5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd5` | 100% NULL | 100.0% NULL, 0 val. |
| `zt5` | 100% NULL | 100.0% NULL, 0 val. |
| `za6` | 100% NULL | 100.0% NULL, 0 val. |
| `zn6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd6` | 100% NULL | 100.0% NULL, 0 val. |
| `zt6` | 100% NULL | 100.0% NULL, 0 val. |
| `za7` | 100% NULL | 100.0% NULL, 0 val. |
| `zn7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd7` | 100% NULL | 100.0% NULL, 0 val. |
| `zt7` | 100% NULL | 100.0% NULL, 0 val. |
| `za8` | 100% NULL | 100.0% NULL, 0 val. |
| `zn8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd8` | 100% NULL | 100.0% NULL, 0 val. |
| `zt8` | 100% NULL | 100.0% NULL, 0 val. |
| `za9` | 100% NULL | 100.0% NULL, 0 val. |
| `zn9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd9` | 100% NULL | 100.0% NULL, 0 val. |
| `zt9` | 100% NULL | 100.0% NULL, 0 val. |
| `ze1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_courante` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_autre` | 100% NULL | 100.0% NULL, 0 val. |
| `index_c` | 100% NULL | 100.0% NULL, 0 val. |
| `index_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_l` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_d` | 100% NULL | 100.0% NULL, 0 val. |
| `index_t` | 100% NULL | 100.0% NULL, 0 val. |
| `index_e` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_lisval` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |


### 📦 lisval_slimstock

| Métrique | Valeur |
|----------|--------|
| Lignes | 194,493 |
| Colonnes ODS | 80 |
| Colonnes PREP | 6 |
| Exclues | 74 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `typ_fich` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `liste` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zn0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd0` | 100% NULL | 100.0% NULL, 0 val. |
| `zc0` | 100% NULL | 100.0% NULL, 0 val. |
| `za1` | 100% NULL | 100.0% NULL, 0 val. |
| `zn1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd1` | 100% NULL | 100.0% NULL, 0 val. |
| `zt1` | 100% NULL | 100.0% NULL, 0 val. |
| `za2` | 100% NULL | 100.0% NULL, 0 val. |
| `zn2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd2` | 100% NULL | 100.0% NULL, 0 val. |
| `zt2` | 100% NULL | 100.0% NULL, 0 val. |
| `za3` | 100% NULL | 100.0% NULL, 0 val. |
| `zn3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd3` | 100% NULL | 100.0% NULL, 0 val. |
| `zt3` | 100% NULL | 100.0% NULL, 0 val. |
| `za4` | 100% NULL | 100.0% NULL, 0 val. |
| `zn4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd4` | 100% NULL | 100.0% NULL, 0 val. |
| `zt4` | 100% NULL | 100.0% NULL, 0 val. |
| `za5` | 100% NULL | 100.0% NULL, 0 val. |
| `zn5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd5` | 100% NULL | 100.0% NULL, 0 val. |
| `zt5` | 100% NULL | 100.0% NULL, 0 val. |
| `za6` | 100% NULL | 100.0% NULL, 0 val. |
| `zn6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd6` | 100% NULL | 100.0% NULL, 0 val. |
| `zt6` | 100% NULL | 100.0% NULL, 0 val. |
| `za7` | 100% NULL | 100.0% NULL, 0 val. |
| `zn7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd7` | 100% NULL | 100.0% NULL, 0 val. |
| `zt7` | 100% NULL | 100.0% NULL, 0 val. |
| `za8` | 100% NULL | 100.0% NULL, 0 val. |
| `zn8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd8` | 100% NULL | 100.0% NULL, 0 val. |
| `zt8` | 100% NULL | 100.0% NULL, 0 val. |
| `za9` | 100% NULL | 100.0% NULL, 0 val. |
| `zn9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zl9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zd9` | 100% NULL | 100.0% NULL, 0 val. |
| `zt9` | 100% NULL | 100.0% NULL, 0 val. |
| `ze0` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze8` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ze9` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cle_courante` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ordre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_autre` | 100% NULL | 100.0% NULL, 0 val. |
| `index_c` | 100% NULL | 100.0% NULL, 0 val. |
| `index_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_l` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `index_d` | 100% NULL | 100.0% NULL, 0 val. |
| `index_t` | 100% NULL | 100.0% NULL, 0 val. |
| `index_e` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_lisval` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |


### 📦 lldespro

| Métrique | Valeur |
|----------|--------|
| Lignes | 551,535 |
| Colonnes ODS | 9 |
| Colonnes PREP | 5 |
| Exclues | 4 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `TabPart_lldespro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |


### 📦 prmultfo

| Métrique | Valeur |
|----------|--------|
| Lignes | 873 |
| Colonnes ODS | 172 |
| Colonnes PREP | 25 |
| Exclues | 147 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `clas_fou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `devise` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pxnet` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_fpxa` | Valeur constante (1 seule valeur) | 35.4% NULL, 1 val. |
| `uni_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_unia` | Cardinalité très faible (2 valeurs: ['pce', 'PCE']) | 1.1% NULL, 2 val. |
| `lib_cona` | Valeur constante (1 seule valeur) | 99.5% NULL, 1 val. |
| `lib_sura` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_unia` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cona` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `frais_app` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_sto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ach_spe` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_elem` | Cardinalité très faible (2 valeurs: ['ARC', 'AR']) | 0.0% NULL, 2 val. |
| `statut` | Cardinalité très faible (2 valeurs: [8, 0]) | 0.0% NULL, 2 val. |
| `conv_md` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_dec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gest_uc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `longueur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `largeur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hauteur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_unic` | 100% NULL | 100.0% NULL, 0 val. |
| `mini_rea` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `conv_pa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `multiple` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jalon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `garantie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jour_gar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `unit_gar` | 100% NULL | 100.0% NULL, 0 val. |
| `ref_fab` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_rtf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_tar` | Cardinalité très faible (2 valeurs: ['09', 'ACH']) | 3.8% NULL, 2 val. |
| `dur_dec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reg_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_prin` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pp_ua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ndos` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `civ_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adr_fac_3` | 100% NULL | 100.0% NULL, 0 val. |
| `villef` | 100% NULL | 100.0% NULL, 0 val. |
| `paysf` | 100% NULL | 100.0% NULL, 0 val. |
| `k_post2f` | 100% NULL | 100.0% NULL, 0 val. |
| `fpxnet` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retour_date` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dgc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dlc_der` | 100% NULL | 100.0% NULL, 0 val. |
| `art_cs_u` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_c` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_s` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_frapp` | 100% NULL | 100.0% NULL, 0 val. |
| `np_arr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_rem_ach` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_fam_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `f_s4_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pp_ua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_miniach` | 100% NULL | 100.0% NULL, 0 val. |
| `f_multach` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_import` | Cardinalité très faible (2 valeurs: [datetime.date(2025, 12, 2), datetime.date(2025, 12, 1)]) | 0.0% NULL, 2 val. |
| `part_fou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_etiq` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_pal` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_eco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_ua1` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_ua2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_ua3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_ua1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_ua2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_ca3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `refrec` | 100% NULL | 100.0% NULL, 0 val. |
| `refrec2` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_pa` | Valeur constante (1 seule valeur) | 90.1% NULL, 1 val. |
| `uni_poi` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_6` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_upa_7` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_6` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ua_7` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite1` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite2` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite3` | 100% NULL | 100.0% NULL, 0 val. |
| `fl_qualite4` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_uc` | Valeur constante (1 seule valeur) | 90.1% NULL, 1 val. |
| `TabPart_prmultfo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `f_poi` | 100% NULL | 100.0% NULL, 0 val. |
| `adrfac4` | 100% NULL | 100.0% NULL, 0 val. |
| `adrfac5` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_edi` | Valeur constante (1 seule valeur) | 90.1% NULL, 1 val. |
| `uni_pcb_edi` | 100% NULL | 100.0% NULL, 0 val. |
| `f_qteeco` | 100% NULL | 100.0% NULL, 0 val. |
| `np_poids` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 prnomenc

| Métrique | Valeur |
|----------|--------|
| Lignes | 18,877 |
| Colonnes ODS | 137 |
| Colonnes PREP | 13 |
| Exclues | 124 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `vte_comp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_comp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_pert` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ent_comp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_sor` | 100% NULL | 100.0% NULL, 0 val. |
| `condition` | Cardinalité très faible (2 valeurs: ['A', 'T']) | 72.7% NULL, 2 val. |
| `stand_by` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `variante` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mult_niv` | 100% NULL | 100.0% NULL, 0 val. |
| `f_qte` | 100% NULL | 100.0% NULL, 0 val. |
| `dat_app` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_stk` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_unis` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_cons` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_surs` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_unis` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cons` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_rec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_trt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pose` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `f_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `repere` | 100% NULL | 100.0% NULL, 0 val. |
| `phase` | 100% NULL | 100.0% NULL, 0 val. |
| `palier` | 100% NULL | 100.0% NULL, 0 val. |
| `fcod_nmc` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_cmp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lig_ordo` | 100% NULL | 100.0% NULL, 0 val. |
| `des_tech` | >95% NULL (100.0%) + 2 valeur(s) distincte(s) | 100.0% NULL, 2 val. |
| `lien_rtf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_qte` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_avc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tps_trsf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dep_cons` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_pds` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_pds` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dem_emp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `emplac` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_us` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_pf` | Valeur constante (1 seule valeur) | 98.4% NULL, 1 val. |
| `famille` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s_famille` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famille` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s4_famille` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_pert` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_cai` | >95% NULL (100.0%) + 2 valeur(s) distincte(s) | 100.0% NULL, 2 val. |
| `no_page` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `inc_deb` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `date_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `date_fin` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_1` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_2` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_3` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_4` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_5` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_6` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_s_7` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_fin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `val_pert` | 100% NULL | 100.0% NULL, 0 val. |
| `no_info` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tol_form` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tol_forp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_us1` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_us2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_us3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_us1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_us2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cs3` | Cardinalité très faible (2 valeurs: [Decimal('1.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `coef_px` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mo_ensai` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `val_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `res_crit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gratuit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `regl_acq` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_gra` | 100% NULL | 100.0% NULL, 0 val. |
| `ret_stk` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `val_nulle` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `part_entr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grp_subs` | 100% NULL | 100.0% NULL, 0 val. |
| `int_subst` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_prnomenc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 produit

| Métrique | Valeur |
|----------|--------|
| Lignes | 1,617 |
| Colonnes ODS | 618 |
| Colonnes PREP | 54 |
| Exclues | 564 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `sous_type` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `clas_grp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `clas_fam` | Cardinalité très faible (2 valeurs: [3, 0]) | 0.0% NULL, 2 val. |
| `clas_sf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `calcul` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reg_fact` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `repert` | 100% NULL | 100.0% NULL, 0 val. |
| `val_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `crit_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_t2` | Cardinalité très faible (2 valeurs: [Decimal('100.00'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `coef_t3` | Cardinalité très faible (2 valeurs: [Decimal('100.00'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `coef_t4` | Cardinalité très faible (2 valeurs: [Decimal('100.00'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `fpx_refv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fcoef_t2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fcoef_t3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fcoef_t4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_fpxv` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lib_conv` | >95% NULL (99.4%) + 3 valeur(s) distincte(s) | 99.4% NULL, 3 val. |
| `lib_surv` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_univ` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `des_cat` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_pan` | 100% NULL | 100.0% NULL, 0 val. |
| `poi_rel` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `perempt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `immatr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_point` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `equiv` | 100% NULL | 100.0% NULL, 0 val. |
| `code_tva` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `enc_db` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `conv_px` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `modelv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ctrl_qua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `suiv_lot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `suiv_se` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_es` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_for` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_for` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_c` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_s` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poid_brut_1` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_brut_2` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_brut_3` | 100% NULL | 100.0% NULL, 0 val. |
| `largeur_1` | 100% NULL | 100.0% NULL, 0 val. |
| `largeur_2` | 100% NULL | 100.0% NULL, 0 val. |
| `largeur_3` | 100% NULL | 100.0% NULL, 0 val. |
| `longueur_1` | 100% NULL | 100.0% NULL, 0 val. |
| `longueur_2` | 100% NULL | 100.0% NULL, 0 val. |
| `longueur_3` | 100% NULL | 100.0% NULL, 0 val. |
| `hauteur_1` | 100% NULL | 100.0% NULL, 0 val. |
| `hauteur_2` | 100% NULL | 100.0% NULL, 0 val. |
| `hauteur_3` | 100% NULL | 100.0% NULL, 0 val. |
| `couche_1` | 100% NULL | 100.0% NULL, 0 val. |
| `couche_2` | 100% NULL | 100.0% NULL, 0 val. |
| `couche_3` | 100% NULL | 100.0% NULL, 0 val. |
| `des_cat2` | 100% NULL | 100.0% NULL, 0 val. |
| `modela` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `heure` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_rvt` | Cardinalité très faible (2 valeurs: [Decimal('117.8900'), Decimal('0.0000')]) | 0.0% NULL, 2 val. |
| `tx_com` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `decl_fou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poi_var` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_av` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_refav` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `chg_jour` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `code_tra` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_par` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dlv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `a_saisie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `v_saisie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `a_ordre_1` | 100% NULL | 100.0% NULL, 0 val. |
| `a_ordre_2` | 100% NULL | 100.0% NULL, 0 val. |
| `a_ordre_3` | 100% NULL | 100.0% NULL, 0 val. |
| `a_ordre_4` | 100% NULL | 100.0% NULL, 0 val. |
| `a_ordre_5` | 100% NULL | 100.0% NULL, 0 val. |
| `v_ordre_1` | 100% NULL | 100.0% NULL, 0 val. |
| `v_ordre_2` | 100% NULL | 100.0% NULL, 0 val. |
| `v_ordre_3` | 100% NULL | 100.0% NULL, 0 val. |
| `v_ordre_4` | 100% NULL | 100.0% NULL, 0 val. |
| `v_ordre_5` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_imp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rem_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fac_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `page_cat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `garantie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jour_gar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `grille` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `classe_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `un_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `grp_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `risq_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `radio_dng` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ps_etiq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_rfa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `assoc_gp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mode_gp` | 100% NULL | 100.0% NULL, 0 val. |
| `prod_gp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arret_gp` | 100% NULL | 100.0% NULL, 0 val. |
| `ref_fab` | 100% NULL | 100.0% NULL, 0 val. |
| `int_condt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gar_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_gar` | 100% NULL | 100.0% NULL, 0 val. |
| `f_poi` | 100% NULL | 100.0% NULL, 0 val. |
| `f_vol` | 100% NULL | 100.0% NULL, 0 val. |
| `f_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_eco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_j_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `magasin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `emplac` | 100% NULL | 100.0% NULL, 0 val. |
| `mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `maxi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `regr_fab` | 100% NULL | 100.0% NULL, 0 val. |
| `gam_pro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nmc_pro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_for` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_pon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_var_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_var_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_var_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pxr` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_sta` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_4` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_5` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_6` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_7` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_8` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_9` | 100% NULL | 100.0% NULL, 0 val. |
| `f_div_10` | 100% NULL | 100.0% NULL, 0 val. |
| `lieu_conso` | 100% NULL | 100.0% NULL, 0 val. |
| `lieu_type` | 100% NULL | 100.0% NULL, 0 val. |
| `mult_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mult_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `densite` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `res_qua` | 100% NULL | 100.0% NULL, 0 val. |
| `dep_qua` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_sav` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_fich` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_gar` | 100% NULL | 100.0% NULL, 0 val. |
| `unit_gar` | 100% NULL | 100.0% NULL, 0 val. |
| `ext_gar` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `del_int` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fou_sav` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `qte_serm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `concurrent` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_dlv` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_ctr` | 100% NULL | 100.0% NULL, 0 val. |
| `px_sim` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `qte_max` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_imm` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_loc` | 100% NULL | 100.0% NULL, 0 val. |
| `val_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `refrec` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `survei` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `equiv_pc` | 100% NULL | 100.0% NULL, 0 val. |
| `sug_imm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sug_lot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prep_isol` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `icpe_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `dep_uniq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_ligv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_ligv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_ligv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_liga_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_liga_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_liga_3` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_imm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_fac` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_conv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_cal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_cal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_cal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_cal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_cal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_cal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_cal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_cal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_cal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_cal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coeff_pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ctr_cai` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_tar` | 100% NULL | 100.0% NULL, 0 val. |
| `lim_sta` | Valeur constante (1 seule valeur) | 99.9% NULL, 1 val. |
| `volume_1` | 100% NULL | 100.0% NULL, 0 val. |
| `volume_2` | 100% NULL | 100.0% NULL, 0 val. |
| `volume_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_blq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mini_vte` | Cardinalité très faible (2 valeurs: [Decimal('1.00000'), Decimal('0.00000')]) | 0.0% NULL, 2 val. |
| `crit_cli_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_nue` | 100% NULL | 100.0% NULL, 0 val. |
| `mult_pro` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pp_uv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reappro` | 100% NULL | 100.0% NULL, 0 val. |
| `res_aut` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_nbfab` | 100% NULL | 100.0% NULL, 0 val. |
| `pou_poid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_prx` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_prx` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_smp` | Valeur constante (1 seule valeur) | 3.8% NULL, 1 val. |
| `pou_spoid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tare` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `catego` | 100% NULL | 100.0% NULL, 0 val. |
| `px_std` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_ctrl` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_mat` | 100% NULL | 100.0% NULL, 0 val. |
| `materiel` | 100% NULL | 100.0% NULL, 0 val. |
| `tps_unit_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tps_unit_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tps_unit_3` | 100% NULL | 100.0% NULL, 0 val. |
| `taxe_eee` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_eee` | 100% NULL | 100.0% NULL, 0 val. |
| `type_eee` | 100% NULL | 100.0% NULL, 0 val. |
| `fct_cal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `fct_cal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fct_cal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fct_cal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fct_cal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pb_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pb_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pb_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_vl_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_vl_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_vl_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pn` | 100% NULL | 100.0% NULL, 0 val. |
| `not_sor_lot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cbn_for` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cbn_n` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cbn_pon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cbn_var_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cbn_var_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cbn_var_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_coef` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cbn_coef` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fou_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cal_nbfab` | 100% NULL | 100.0% NULL, 0 val. |
| `degre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `contenance` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `inc_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `f_tr` | 100% NULL | 100.0% NULL, 0 val. |
| `ps_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dgc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rac_lot` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rac_imm` | 100% NULL | 100.0% NULL, 0 val. |
| `mult_vp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `exc_can` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_remp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ns_indrev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typeve` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rem_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_perte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_l_cpt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lanc_sce` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_cat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_bois` | 100% NULL | 100.0% NULL, 0 val. |
| `px_ttc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `er_div_1` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_2` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_3` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_4` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_5` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_6` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_7` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_8` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_9` | 100% NULL | 100.0% NULL, 0 val. |
| `er_div_10` | 100% NULL | 100.0% NULL, 0 val. |
| `f_mv` | 100% NULL | 100.0% NULL, 0 val. |
| `f_uv` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cv` | 100% NULL | 100.0% NULL, 0 val. |
| `res_achv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `acheteur` | 100% NULL | 100.0% NULL, 0 val. |
| `dlc_fm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `lot_orim` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sem_couv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prod_form` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pesee_cai` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `couv_sou` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rendement` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `car_lcq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `car_lcq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `car_lcq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `car_lcq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `car_lcq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `duree` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pr_stat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_durgar` | 100% NULL | 100.0% NULL, 0 val. |
| `list_serv` | 100% NULL | 100.0% NULL, 0 val. |
| `ing_abs` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ing_lib1` | 100% NULL | 100.0% NULL, 0 val. |
| `ing_lib2` | 100% NULL | 100.0% NULL, 0 val. |
| `ing_grp` | 100% NULL | 100.0% NULL, 0 val. |
| `ing_car` | 100% NULL | 100.0% NULL, 0 val. |
| `som_cmp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_forf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `maqf_etq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `part_remp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tunnel_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `trans_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `gc_typmc` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_etiq` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_sscc` | 100% NULL | 100.0% NULL, 0 val. |
| `gc_exutoire` | 100% NULL | 100.0% NULL, 0 val. |
| `code_bio` | >95% NULL (99.9%) + 2 valeur(s) distincte(s) | 99.9% NULL, 2 val. |
| `poids_prepa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pou_s3poid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pou_s4poid` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mult_stock` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sfam_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3fam_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `t_batch_1` | 100% NULL | 100.0% NULL, 0 val. |
| `t_batch_2` | 100% NULL | 100.0% NULL, 0 val. |
| `t_batch_3` | 100% NULL | 100.0% NULL, 0 val. |
| `t_batch_4` | 100% NULL | 100.0% NULL, 0 val. |
| `t_batch_5` | 100% NULL | 100.0% NULL, 0 val. |
| `t_batch_6` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv1` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv2` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_uv3` | 100% NULL | 100.0% NULL, 0 val. |
| `nb_uv1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_uv2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_cv3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `art_cs_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `poid_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `poid_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `larg_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `larg_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `larg_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `long_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `long_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `long_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `haut_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `haut_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `haut_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `vol_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `vol_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `vol_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_p_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_p_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_p_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_v_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_v_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_v_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `vocal_6` | 100% NULL | 100.0% NULL, 0 val. |
| `poids_fin_prepa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `des_voc` | 100% NULL | 100.0% NULL, 0 val. |
| `detrompeur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_ade` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_sto` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_sto` | 100% NULL | 100.0% NULL, 0 val. |
| `ges_rot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `emp_typ` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `t_batcm_1` | 100% NULL | 100.0% NULL, 0 val. |
| `t_batcm_2` | 100% NULL | 100.0% NULL, 0 val. |
| `c_cbn_brut` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_cbn_lser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_etud` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fnb_j_fab` | 100% NULL | 100.0% NULL, 0 val. |
| `crit_param` | 100% NULL | 100.0% NULL, 0 val. |
| `crit_val` | 100% NULL | 100.0% NULL, 0 val. |
| `hier_transfo` | 100% NULL | 100.0% NULL, 0 val. |
| `imm_u_sor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `refrec2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_tax_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_vte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_rvt_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_mul_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_v_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_v_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_v_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_v_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_v_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_r_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_r_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_r_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_r_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_r_5` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_m_1` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_m_2` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_m_3` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_m_4` | 100% NULL | 100.0% NULL, 0 val. |
| `df_f_m_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fct_deb` | 100% NULL | 100.0% NULL, 0 val. |
| `val_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s4fam_fab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pr_tarif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_dluo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nsa_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_cdlc` | 100% NULL | 100.0% NULL, 0 val. |
| `classe2_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `reg_blo_a` | 100% NULL | 100.0% NULL, 0 val. |
| `qli_dng` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reg_blo_d` | 100% NULL | 100.0% NULL, 0 val. |
| `env_dng` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `regl_acq` | 100% NULL | 100.0% NULL, 0 val. |
| `picto_dng` | 100% NULL | 100.0% NULL, 0 val. |
| `grp_subs` | 100% NULL | 100.0% NULL, 0 val. |
| `ori_desass` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_desass` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rarr_cons` | 100% NULL | 100.0% NULL, 0 val. |
| `bloq_prep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rayon` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_pv` | 100% NULL | 100.0% NULL, 0 val. |
| `hr_nb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_stk` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_stat` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_gp` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ust_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ust_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ust_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ust_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ust_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ust_6` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ugp_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ugp_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ugp_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ugp_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ugp_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_ugp_6` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_p_3` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `tare_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_1` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_2` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_3` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_ul_1` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_ul_2` | 100% NULL | 100.0% NULL, 0 val. |
| `pds_net_ul_3` | 100% NULL | 100.0% NULL, 0 val. |
| `lib_bar` | 100% NULL | 100.0% NULL, 0 val. |
| `ssfraisfl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `foufraisfl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cons_auto` | 100% NULL | 100.0% NULL, 0 val. |
| `novtefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `val_pert` | 100% NULL | 100.0% NULL, 0 val. |
| `pose` | 100% NULL | 100.0% NULL, 0 val. |
| `f_pert` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_6` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_uv_7` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_6` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_up_7` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_1` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_2` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_3` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_4` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_5` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_6` | 100% NULL | 100.0% NULL, 0 val. |
| `gst_us_7` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_poi` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_uc` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_lot` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_produit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `apeser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `uni_pvc` | 100% NULL | 100.0% NULL, 0 val. |
| `f_etud` | 100% NULL | 100.0% NULL, 0 val. |
| `sup_cons` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_car` | 100% NULL | 100.0% NULL, 0 val. |
| `pro_prev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_prev` | 100% NULL | 100.0% NULL, 0 val. |
| `dej_col_1` | 100% NULL | 100.0% NULL, 0 val. |
| `dej_col_2` | 100% NULL | 100.0% NULL, 0 val. |
| `dej_col_3` | 100% NULL | 100.0% NULL, 0 val. |
| `dej_col_4` | 100% NULL | 100.0% NULL, 0 val. |
| `dej_col_5` | 100% NULL | 100.0% NULL, 0 val. |
| `dej_col_6` | 100% NULL | 100.0% NULL, 0 val. |
| `stat_remp` | 100% NULL | 100.0% NULL, 0 val. |
| `art_ref` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `magmeub` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_rvt` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_fpxr` | 100% NULL | 100.0% NULL, 0 val. |
| `stk_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `stk_maxi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `classm` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_edi` | 100% NULL | 100.0% NULL, 0 val. |
| `uni_pcb_edi` | 100% NULL | 100.0% NULL, 0 val. |
| `fam_sai` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `del_iapp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nbsem_na` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nbsem_eca` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_turb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cpv` | 100% NULL | 100.0% NULL, 0 val. |
| `ges_titresto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 prprixv

| Métrique | Valeur |
|----------|--------|
| Lignes | 4,994,280 |
| Colonnes ODS | 121 |
| Colonnes PREP | 8 |
| Exclues | 113 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `coef_t2` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('100.00')]) | 0.0% NULL, 2 val. |
| `coef_t3` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('100.00')]) | 0.0% NULL, 2 val. |
| `coef_t4` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('100.00')]) | 0.0% NULL, 2 val. |
| `fcoef_t2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fcoef_t3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fcoef_t4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cou_fix` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `qte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `px_vte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `fpx_vte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `fcoef_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coeff_pvc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `depot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coeff` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fpx_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_6` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_7` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_8` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_9` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_10` | 100% NULL | 100.0% NULL, 0 val. |
| `coef_av` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `qte_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `qte_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_1` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_3` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_4` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_6` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_7` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_8` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_9` | 100% NULL | 100.0% NULL, 0 val. |
| `fqte_rq_10` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_rvt_vte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_prprixv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 soumission

| Métrique | Valeur |
|----------|--------|
| Lignes | 176,053 |
| Colonnes ODS | 154 |
| Colonnes PREP | 16 |
| Exclues | 138 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `remise1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_1` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_2` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_3` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_app_4` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_1` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_2` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_3` | 100% NULL | 100.0% NULL, 0 val. |
| `px_net_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec1` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec2` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec3` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec4` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_dec5` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem1_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `exclusif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `md5` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_1` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_2` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_3` | 100% NULL | 100.0% NULL, 0 val. |
| `f_cl_4` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `remise4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `lien_ach` | >95% NULL (100.0%) + 2 valeur(s) distincte(s) | 100.0% NULL, 2 val. |
| `depot` | Cardinalité très faible (2 valeurs: [0, 41]) | 0.0% NULL, 2 val. |
| `M_dat_1` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_2` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_3` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_4` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_5` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_6` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_7` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_8` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_9` | 100% NULL | 100.0% NULL, 0 val. |
| `M_dat_10` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_1` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_2` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_3` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_4` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_5` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_6` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_7` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_8` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_9` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qte_10` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_1` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_2` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_3` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_4` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_5` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_6` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_7` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_8` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_9` | 100% NULL | 100.0% NULL, 0 val. |
| `m_qui_10` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_rem4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ope_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_rfa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `eve_exc` | 100% NULL | 100.0% NULL, 0 val. |
| `groupement` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `national` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `regional` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_3` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_4` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_1` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_2` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_3` | 100% NULL | 100.0% NULL, 0 val. |
| `grille_rem_4` | 100% NULL | 100.0% NULL, 0 val. |
| `en-tete` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `pere` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `continental` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_soumission` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `region_depots` | 100% NULL | 100.0% NULL, 0 val. |
| `statut` | 100% NULL | 100.0% NULL, 0 val. |
| `principal` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `achev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 stock

| Métrique | Valeur |
|----------|--------|
| Lignes | 278,916 |
| Colonnes ODS | 45 |
| Colonnes PREP | 26 |
| Exclues | 19 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `maxi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coef_dep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fut_stock` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_std3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `px_sim` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `detrompeur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vpm` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vpr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `TabPart_stock` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `s_secu` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s_alerte` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s_pt_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `dat_cal` | 100% NULL | 100.0% NULL, 0 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |

**⚠️ Colonnes corrélées détectées :**

- `poids` ↔️ `poids_net` (score: 99.84%)
- `poi_sit` ↔️ `poi_net_sit` (score: 99.84%)


### 📦 tabgco

| Métrique | Valeur |
|----------|--------|
| Lignes | 6,116 |
| Colonnes ODS | 291 |
| Colonnes PREP | 32 |
| Exclues | 259 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `ml_mtfra` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ml_port` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `to_chauf` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_2` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_1` | 100% NULL | 100.0% NULL, 0 val. |
| `znu_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_1` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_2` | 100% NULL | 100.0% NULL, 0 val. |
| `adresse_3` | 100% NULL | 100.0% NULL, 0 val. |
| `k_post` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `code_df` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_com` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ticket` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cli_cai` | Cardinalité très faible (2 valeurs: [0, 1245]) | 0.0% NULL, 2 val. |
| `typ_dec` | >95% NULL (99.1%) + 2 valeur(s) distincte(s) | 99.1% NULL, 2 val. |
| `cumul_z` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_int_1` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_2` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_3` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_4` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_5` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_6` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_7` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_8` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_9` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_int_10` | 100% NULL | 100.0% NULL, 0 val. |
| `presence` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_int` | Cardinalité très faible (2 valeurs: [16, 0]) | 0.0% NULL, 2 val. |
| `mdp_resp` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_equ` | >95% NULL (99.1%) + 2 valeur(s) distincte(s) | 99.1% NULL, 2 val. |
| `matric` | >95% NULL (99.1%) + 2 valeur(s) distincte(s) | 99.1% NULL, 2 val. |
| `coef_chg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `heur_trav` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_heur` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `depot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_tel_1` | 100% NULL | 100.0% NULL, 0 val. |
| `no_tel_2` | 100% NULL | 100.0% NULL, 0 val. |
| `no_tel_3` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fax_1` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fax_2` | 100% NULL | 100.0% NULL, 0 val. |
| `no_fax_3` | 100% NULL | 100.0% NULL, 0 val. |
| `type_tf_1` | 100% NULL | 100.0% NULL, 0 val. |
| `type_tf_2` | 100% NULL | 100.0% NULL, 0 val. |
| `type_tf_3` | 100% NULL | 100.0% NULL, 0 val. |
| `statut` | >95% NULL (98.2%) + 3 valeur(s) distincte(s) | 98.2% NULL, 3 val. |
| `no_port` | Valeur constante (1 seule valeur) | 99.6% NULL, 1 val. |
| `coef_av` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `capacite` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fp_cptg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fp_cptl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cpt_fvt_1` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_2` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_3` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_4` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_5` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_6` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_7` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_fvt_8` | 100% NULL | 100.0% NULL, 0 val. |
| `min_t` | Cardinalité très faible (2 valeurs: [5, 0]) | 0.0% NULL, 2 val. |
| `jour_t_1` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_t_2` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_t_3` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_t_4` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_t_5` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_t_6` | 100% NULL | 100.0% NULL, 0 val. |
| `jour_t_7` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_1` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_2` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_3` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_4` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_5` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_6` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_t_7` | 100% NULL | 100.0% NULL, 0 val. |
| `chef_equ` | 100% NULL | 100.0% NULL, 0 val. |
| `cod_tlv` | 100% NULL | 100.0% NULL, 0 val. |
| `login` | >95% NULL (100.0%) + 3 valeur(s) distincte(s) | 100.0% NULL, 3 val. |
| `usr_pro` | 100% NULL | 100.0% NULL, 0 val. |
| `sect_ana_1` | 100% NULL | 100.0% NULL, 0 val. |
| `sect_ana_2` | 100% NULL | 100.0% NULL, 0 val. |
| `fp_cptc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grille_pan` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rp_prc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_am1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_am2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_pm1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_pm2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `contact` | 100% NULL | 100.0% NULL, 0 val. |
| `h_niv1` | 100% NULL | 100.0% NULL, 0 val. |
| `h_niv2` | 100% NULL | 100.0% NULL, 0 val. |
| `h_niv3` | Valeur constante (1 seule valeur) | 99.8% NULL, 1 val. |
| `h_niv4` | 100% NULL | 100.0% NULL, 0 val. |
| `not_chx_emp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_6` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_7` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_8` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_9` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_10` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_11` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_12` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_13` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_14` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_15` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_16` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_17` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_18` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_19` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_20` | 100% NULL | 100.0% NULL, 0 val. |
| `pref_ref` | 100% NULL | 100.0% NULL, 0 val. |
| `dgc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `longitude` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `latitude` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `heure_2_1` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_2_2` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_2_3` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_2_4` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_2_5` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_2_6` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_2_7` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_1` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_2` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_3` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_4` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_5` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_6` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_3_7` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_1` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_2` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_3` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_4` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_5` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_6` | 100% NULL | 100.0% NULL, 0 val. |
| `heure_4_7` | 100% NULL | 100.0% NULL, 0 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `WEC` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `datsor` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `zon_lib_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zon_lib_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `code_tiers` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `compte_tiers` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `location` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `sans_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `magasin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `paye_ndos` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `paye_etab` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_temps` | 100% NULL | 100.0% NULL, 0 val. |
| `trf_paie` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `notel` | >95% NULL (99.6%) + 2 valeur(s) distincte(s) | 99.6% NULL, 2 val. |
| `nopor` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `secta_imp_1` | 100% NULL | 100.0% NULL, 0 val. |
| `secta_imp_2` | 100% NULL | 100.0% NULL, 0 val. |
| `dep_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `comp_reg_1` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_2` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_3` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_4` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_5` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_6` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_7` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_8` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_9` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_10` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_11` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_12` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_13` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_14` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_15` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_16` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_17` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_18` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_19` | 100% NULL | 100.0% NULL, 0 val. |
| `comp_reg_20` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_1` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_2` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_3` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_4` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_5` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_6` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_7` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_8` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_9` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_10` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_11` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_12` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_13` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_14` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_15` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_16` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_17` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_18` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_19` | 100% NULL | 100.0% NULL, 0 val. |
| `ord_reg_20` | 100% NULL | 100.0% NULL, 0 val. |
| `dep_locq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mcalpx` | 100% NULL | 100.0% NULL, 0 val. |
| `gen_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_index_chemin` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nb_palettes_h` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `vent_ate` | 100% NULL | 100.0% NULL, 0 val. |
| `rei_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nai_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `reg_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `nat_deb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_dug_fac` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `non_dug_avo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bloq_prep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gencod` | 100% NULL | 100.0% NULL, 0 val. |
| `min_dep_to` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `not_ges_hr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_nb` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_lot` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_tabgco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `promo_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cess_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retr_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adresse5` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_1` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_2` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_3` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_4` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_5` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_6` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_7` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_8` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_9` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_10` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_11` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_12` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_13` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_14` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_15` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_16` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_17` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_18` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_19` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_evo_20` | 100% NULL | 100.0% NULL, 0 val. |
| `fp_cptf` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |


### 📦 typelem

| Métrique | Valeur |
|----------|--------|
| Lignes | 43 |
| Colonnes ODS | 153 |
| Colonnes PREP | 11 |
| Exclues | 142 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `ndos` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `chap_util_1` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_2` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_3` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_4` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_5` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_6` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_7` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_8` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_9` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_10` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_11` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_12` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_13` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_14` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_15` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_16` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_17` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_18` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_19` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_20` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_21` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_22` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_23` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_24` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_25` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_26` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_27` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_28` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_29` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_30` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_31` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_32` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_33` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_34` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_35` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_36` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_37` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_38` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_39` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_40` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_41` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_42` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_43` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_44` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_45` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_46` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_47` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_48` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_49` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_50` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_51` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_52` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_53` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_54` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_55` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_56` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_57` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_58` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_59` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_60` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_61` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_62` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_63` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_64` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_65` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_66` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_67` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_68` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_69` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_70` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_71` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_72` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_73` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_74` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_75` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_76` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_77` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_78` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_79` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_80` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_81` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_82` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_83` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_84` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_85` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_86` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_87` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_88` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_89` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_90` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_91` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_92` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_93` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_94` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_95` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_96` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_97` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_98` | 100% NULL | 100.0% NULL, 0 val. |
| `chap_util_99` | 100% NULL | 100.0% NULL, 0 val. |
| `decl_v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `decl_a` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grille-a` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grille-v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `color_bg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `color_fg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `variante` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `actif` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gst_gen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jal_ach` | 100% NULL | 100.0% NULL, 0 val. |
| `d_qa` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `d_qv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ty_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bl_mini` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_gmao` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_bois` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_rend` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_qtemo` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mot-cle` | 100% NULL | 100.0% NULL, 0 val. |
| `chemin` | 100% NULL | 100.0% NULL, 0 val. |
| `particulier` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `modele_fiche` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ges_billon` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typefl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_car` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_typelem` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `ges_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ps_auto` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `gen_dev` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ori_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `nat_cde` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_sui` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_bpc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_loaded_at` | Colonne technique ETL |  |
| `_etl_updated_at` | Colonne technique ETL |  |
| `_etl_source` | Colonne technique ETL |  |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_is_active` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |
| `_etl_valid_to` | Colonne technique ETL |  |

