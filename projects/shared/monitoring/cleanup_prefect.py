"""
Flow de maintenance pour nettoyer la base Prefect
"""

import sys
from pathlib import Path

# Ajouter le projet au path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import asyncio
from datetime import datetime, timedelta, timezone
from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client
# AJOUT : Imports des filtres spécifiques nécessaires
from prefect.client.schemas.filters import (
    FlowRunFilter, 
    TaskRunFilter, 
    DeploymentFilter, 
    DeploymentFilterId
)
from prefect.client.schemas.sorting import FlowRunSort



# ---------------------------------------------------------------------------
# TASKS
# ---------------------------------------------------------------------------

@task(name="[STATS] Statistiques base Prefect")
async def get_database_stats():
    """Affiche statistiques de la base Prefect"""
    logger = get_run_logger()
    
    async with get_client() as client:
        # Compter flow runs (paginer par 200)
        flow_runs_count = 0
        offset = 0
        while True:
            batch = await client.read_flow_runs(limit=200, offset=offset)
            if not batch:
                break
            flow_runs_count += len(batch)
            offset += 200
            if len(batch) < 200:
                break
        
        # Compter task runs (paginer par 200)
        task_runs_count = 0
        offset = 0
        while True:
            batch = await client.read_task_runs(limit=200, offset=offset)
            if not batch:
                break
            task_runs_count += len(batch)
            offset += 200
            if len(batch) < 200:
                break
        
        # Compter deployments
        deployments = await client.read_deployments()
        
        logger.info("=" * 70)
        logger.info("[STATS] Statistiques base Prefect")
        logger.info("=" * 70)
        logger.info(f"  Flow runs     : {flow_runs_count}")
        logger.info(f"  Task runs     : {task_runs_count}")
        logger.info(f"  Deployments   : {len(deployments)}")
        logger.info("=" * 70)
        
        return {
            "flow_runs": flow_runs_count,
            "task_runs": task_runs_count,
            "deployments": len(deployments)
        }


@task(name="[CLEAN] Supprimer anciens flow runs")
async def cleanup_old_flow_runs(days_to_keep=30):
    """Supprime les flow runs plus vieux que X jours"""
    logger = get_run_logger()
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
    logger.info(f"[CLEAN] Suppression runs avant {cutoff_date.strftime('%Y-%m-%d')}")
    
    async with get_client() as client:
        deleted_count = 0
        offset = 0
        
        while True:
            # Lire batch de 200
            flow_runs = await client.read_flow_runs(
                limit=200,
                offset=offset,
                sort=FlowRunSort.EXPECTED_START_TIME_ASC
            )
            
            if not flow_runs:
                break
            
            # Filtrer ceux à supprimer (comparaison timezone-aware)
            to_delete = [
                fr for fr in flow_runs 
                if fr.expected_start_time and fr.expected_start_time < cutoff_date
            ]
            
            if not to_delete:
                pass
            else:
                for fr in to_delete:
                    try:
                        await client.delete_flow_run(fr.id)
                        deleted_count += 1
                    except Exception as e:
                        logger.warning(f"[ERROR] Flow run {fr.id}: {e}")

            if len(flow_runs) < 200:
                break
            
            offset += 200
        
        logger.info(f"[OK] {deleted_count} flow runs supprimés")
        return deleted_count


@task(name="[CLEAN] Supprimer anciens task runs")
async def cleanup_old_task_runs(days_to_keep=30):
    """Supprime les task runs plus vieux que X jours"""
    logger = get_run_logger()
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
    logger.info(f"[CLEAN] Suppression tasks avant {cutoff_date.strftime('%Y-%m-%d')}")
    
    async with get_client() as client:
        deleted_count = 0
        offset = 0
        
        while True:
            task_runs = await client.read_task_runs(
                limit=200,
                offset=offset,
                sort="EXPECTED_START_TIME_ASC"
            )
            
            if not task_runs:
                break
            
            to_delete = [
                tr for tr in task_runs 
                if tr.expected_start_time and tr.expected_start_time < cutoff_date
            ]
            
            for tr in to_delete:
                try:
                    await client.delete_task_run(tr.id)
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"[ERROR] Task run {tr.id}: {e}")
            
            if len(task_runs) < 200:
                break

            offset += 200
        
        logger.info(f"[OK] {deleted_count} task runs supprimés")
        return deleted_count


@task(name="[CLEAN] Garder N derniers runs par deployment")
async def cleanup_excess_runs_per_deployment(keep_last_n=50):
    """Garde seulement les N derniers runs par deployment"""
    logger = get_run_logger()
    
    async with get_client() as client:
        deployments = await client.read_deployments()
        total_deleted = 0
        
        for deployment in deployments:
            # CORRECTION ICI : Utilisation de l'objet DeploymentFilter au lieu d'un dict
            dept_filter = DeploymentFilter(
                id=DeploymentFilterId(any_=[deployment.id])
            )
            
            runs = await client.read_flow_runs(
                deployment_filter=dept_filter,
                limit=200,
                sort=FlowRunSort.EXPECTED_START_TIME_DESC
            )
            
            if len(runs) <= keep_last_n:
                continue
            
            to_delete = runs[keep_last_n:]
            
            for run in to_delete:
                try:
                    await client.delete_flow_run(run.id)
                    total_deleted += 1
                except Exception as e:
                    logger.warning(f"[ERROR] Flow run {run.id}: {e}")
            
            logger.info(f"[{deployment.name}] {len(to_delete)} runs supprimés")
        
        logger.info(f"[OK] Total {total_deleted} runs supprimés")
        return total_deleted


# ---------------------------------------------------------------------------
# MAIN FLOW
# ---------------------------------------------------------------------------

@flow(
    name="[OPS] 🧹 Nettoyage Prefect",
    description="Nettoie la base Prefect : supprime vieux runs et garde N derniers par deployment"
)
async def cleanup_prefect_database_flow(
    days_to_keep: int = 30,
    keep_last_n_per_deployment: int = 50,
    cleanup_tasks: bool = True
):
    """
    Flow principal de nettoyage
    """
    logger = get_run_logger()
    
    logger.info("=" * 70)
    logger.info("[START] Nettoyage base Prefect")
    logger.info("=" * 70)
    logger.info(f"  Jours à conserver       : {days_to_keep}")
    logger.info(f"  Runs par deployment     : {keep_last_n_per_deployment}")
    logger.info(f"  Nettoyer task runs      : {cleanup_tasks}")
    logger.info("=" * 70)
    
    logger.info("")
    logger.info("[BEFORE] Statistiques avant nettoyage")
    stats_before = await get_database_stats()
    
    logger.info("")
    logger.info(f"[STEP 1] Suppression runs > {days_to_keep} jours")
    flow_runs_deleted = await cleanup_old_flow_runs(days_to_keep)
    
    logger.info("")
    logger.info(f"[STEP 2] Garder {keep_last_n_per_deployment} derniers runs/deployment")
    excess_deleted = await cleanup_excess_runs_per_deployment(keep_last_n_per_deployment)
    
    task_runs_deleted = 0
    if cleanup_tasks:
        logger.info("")
        logger.info(f"[STEP 3] Suppression task runs > {days_to_keep} jours")
        task_runs_deleted = await cleanup_old_task_runs(days_to_keep)
    
    logger.info("")
    logger.info("[AFTER] Statistiques après nettoyage")
    stats_after = await get_database_stats()
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("[SUMMARY] Résumé nettoyage")
    logger.info("=" * 70)
    logger.info(f"  Flow runs supprimés     : {flow_runs_deleted + excess_deleted}")
    logger.info(f"  Task runs supprimés     : {task_runs_deleted}")
    logger.info(f"  Flow runs restants      : {stats_after['flow_runs']}")
    logger.info(f"  Task runs restants      : {stats_after['task_runs']}")
    logger.info("=" * 70)
    
    return {
        "flow_runs_deleted": flow_runs_deleted + excess_deleted,
        "task_runs_deleted": task_runs_deleted,
        "stats_before": stats_before,
        "stats_after": stats_after
    }


if __name__ == "__main__":
    asyncio.run(cleanup_prefect_database_flow())