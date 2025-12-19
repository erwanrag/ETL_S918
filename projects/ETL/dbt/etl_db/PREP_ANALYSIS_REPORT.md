# 📊 Rapport d'Analyse PREP - Version Complète

**Généré le** : 2025-12-12 11:28:02

---

## 🎯 Résumé Global

| Métrique | Valeur |
|----------|--------|
| **Tables analysées** | 1 |
| **Lignes totales** | 15,409 |
| **Colonnes ODS** | 316 |
| **Colonnes PREP** | 100 (31.6%) |
| **Colonnes exclues** | 216 (68.4%) |

### 📉 Raisons d'Exclusion

- 🔧 **Colonnes techniques ETL** : 2
- ❌ **Colonnes 100% NULL** : 65
- 📌 **Colonnes constantes** : 144
- ⚠️ **Colonnes faible valeur** : 5
- 🚫 **Blacklist utilisateur** : 0

---

## 📋 Détail par Table

### 📦 client

| Métrique | Valeur |
|----------|--------|
| Lignes | 15,409 |
| Colonnes ODS | 316 |
| Colonnes PREP | 100 |
| Exclues | 216 |

**Colonnes exclues :**

| Colonne | Raison | Stats |
|---------|--------|-------|
| `k_post` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zal_2` | >95% NULL (100.0%) + 2 valeur(s) distincte(s) | 100.0% NULL, 2 val. |
| `zal_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zal_5` | >95% NULL (97.3%) + 2 valeur(s) distincte(s) | 97.3% NULL, 2 val. |
| `znu_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `znu_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zta_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zta_5` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_1` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_2` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_3` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_4` | 100% NULL | 100.0% NULL, 0 val. |
| `zda_5` | 100% NULL | 100.0% NULL, 0 val. |
| `fac_ttc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fra_app` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `div_fac_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `val_fac_2` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('4.00')]) | 0.0% NULL, 2 val. |
| `val_fac_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_art_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_art_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_art_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_art_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fam_art_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `televen` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cal_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cal_liv` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `heure` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `com_tel` | 100% NULL | 100.0% NULL, 0 val. |
| `jou_dec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_livre` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_stat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `app_aff` | 100% NULL | 100.0% NULL, 0 val. |
| `tel_j1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tel_j7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `repert` | 100% NULL | 100.0% NULL, 0 val. |
| `tx_ech_d` | Cardinalité très faible (2 valeurs: [Decimal('1.55000000'), Decimal('0E-8')]) | 0.0% NULL, 2 val. |
| `cl_plnat` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cl_plreg` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tva_ech` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zon_lib` | Valeur constante (1 seule valeur) | 98.0% NULL, 1 val. |
| `rem_glo_1` | Cardinalité très faible (2 valeurs: [Decimal('0.00'), Decimal('4.00')]) | 0.0% NULL, 2 val. |
| `rem_glo_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `coeff_pvc` | Cardinalité très faible (2 valeurs: [Decimal('100.00'), Decimal('0.00')]) | 0.0% NULL, 2 val. |
| `arr_bi_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bi_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bi_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bi_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bi_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bs_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bs_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bs_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bs_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_bs_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_r_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_r_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_r_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_r_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `arr_r_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fac_poi` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `c_factor` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `transit` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `grp_ps` | 100% NULL | 100.0% NULL, 0 val. |
| `zlo_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zlo_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `blok_enc` | Valeur constante (1 seule valeur) | 100.0% NULL, 1 val. |
| `effectif` | Cardinalité très faible (2 valeurs: [0, 95]) | 0.0% NULL, 2 val. |
| `ca_client` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ca_pot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `login` | 100% NULL | 100.0% NULL, 0 val. |
| `log_mdp` | 100% NULL | 100.0% NULL, 0 val. |
| `gencod_det` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ord_affec` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `priorite` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `jalon_tot` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `int_rrr_6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ben_rrr_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ben_rrr_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ben_rrr_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ben_rrr_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ben_rrr_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ben_rrr_6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cadencier` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `interv_1` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_2` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_3` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_4` | 100% NULL | 100.0% NULL, 0 val. |
| `interv_5` | 100% NULL | 100.0% NULL, 0 val. |
| `plus_bpbl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `bl_regr` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tx_com` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mt_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `fr_cde` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `s3_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `s4_famille` | 100% NULL | 100.0% NULL, 0 val. |
| `rem_lig` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cod_tlv` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_a` | 100% NULL | 100.0% NULL, 0 val. |
| `mod_v` | 100% NULL | 100.0% NULL, 0 val. |
| `cal_v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_v` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
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
| `typ_pxa` | Valeur constante (1 seule valeur) | 99.7% NULL, 1 val. |
| `cl_prc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `ffc_prc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rfa_soc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `zone_exp` | 100% NULL | 100.0% NULL, 0 val. |
| `cpt_web` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `f_prc` | 100% NULL | 100.0% NULL, 0 val. |
| `pref_ref` | 100% NULL | 100.0% NULL, 0 val. |
| `dgc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_tar_loc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `meth_rf` | 100% NULL | 100.0% NULL, 0 val. |
| `prc_rep` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `retour_date` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fil_6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cum_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cum_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cum_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cum_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cum_6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_cad` | 100% NULL | 100.0% NULL, 0 val. |
| `dep_unq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `canal` | 100% NULL | 100.0% NULL, 0 val. |
| `notation` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `no_ctrl` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_floc` | 100% NULL | 100.0% NULL, 0 val. |
| `contrat_date` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `date_px` | 100% NULL | 100.0% NULL, 0 val. |
| `typ_cam` | 100% NULL | 100.0% NULL, 0 val. |
| `marge_p` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prix_franco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `rlk_mnt` | >95% NULL (98.4%) + 2 valeur(s) distincte(s) | 98.4% NULL, 2 val. |
| `maqf_etq` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `typ_sup` | 100% NULL | 100.0% NULL, 0 val. |
| `groupe` | >95% NULL (99.7%) + 2 valeur(s) distincte(s) | 99.7% NULL, 2 val. |
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
| `tar_fi2_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fi2_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fi2_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_fi2_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cu2_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cu2_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cu2_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `tar_cu2_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `cal_enc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `prc_aco` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `mod_aco` | 100% NULL | 100.0% NULL, 0 val. |
| `TabPart_client` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `flag_repli` | 100% NULL | 100.0% NULL, 0 val. |
| `jamais_peser` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_1` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_2` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_3` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_4` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_5` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_6` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `hr_app_7` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `multitrp` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `adresse5` | 100% NULL | 100.0% NULL, 0 val. |
| `or_spereg` | 100% NULL | 100.0% NULL, 0 val. |
| `conso_exc` | Valeur constante (1 seule valeur) | 0.0% NULL, 1 val. |
| `_etl_hashdiff` | Colonne technique ETL |  |
| `_etl_valid_from` | Colonne technique ETL |  |

