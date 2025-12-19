"""
============================================================================
Utilitaires Fichiers - Operations robustes Windows/SFTP
============================================================================
Fichier : E:/Prefect/projects/ETL/flows/utils/file_operations.py

Contient :
- safe_move() : Move ultra-robuste avec fallback COPY+DELETE
- archive_and_cleanup() : Archivage Incoming -> Processed + nettoyage SFTP
============================================================================
"""

import os
import shutil
import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict, Optional


def safe_move(src: str, dst: str, retries: int = 30, delay: float = 1.0) -> bool:
    """
    Move ultra-robuste Windows/SFTP avec fallback COPY+DELETE
    
    Args:
        src: Chemin source complet
        dst: Chemin destination complet
        retries: Nombre de tentatives (default: 30)
        delay: Delai entre tentatives en secondes (default: 1.0)
    
    Returns:
        bool: True si succes, False sinon
        
    Strategie :
        1. Tentative MOVE standard (rapide si meme filesystem)
        2. Si PermissionError apres N tentatives -> Fallback COPY+DELETE
        3. Si suppression echoue -> Au moins le fichier est copie (return True)
    """
    # S'assurer que le dossier destination existe
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    
    for attempt in range(retries):
        try:
            # Tentative MOVE standard
            shutil.move(src, dst)
            return True
            
        except PermissionError:
            # Fichier verrouille (typique Windows)
            if attempt == retries - 1:
                # FALLBACK apres toutes les tentatives : COPY puis DELETE
                try:
                    shutil.copy2(src, dst)  # Copie avec metadonnees
                    time.sleep(2)  # Attendre fermeture handles Windows
                    
                    try:
                        os.remove(src)
                        return True
                    except PermissionError:
                        # Fichier copie mais suppression impossible
                        # -> Au moins l'archive existe
                        print(f"Avertissement : Fichier copie mais non supprime : {src}")
                        return True
                        
                except Exception as e:
                    print(f"Erreur : Fallback COPY+DELETE echoue : {e}")
                    return False
            
            # Attendre avant reessayer
            time.sleep(delay)
            
        except FileNotFoundError:
            # Fichier source deja supprime/deplace
            return False
            
        except Exception as e:
            print(f"Erreur : Erreur inattendue lors du move : {e}")
            return False
    
    return False


def archive_and_cleanup(
    base_filename: str,
    archive_root: str,
    incoming_paths: Dict[str, Path],
    sftp_server_paths: Dict[str, Path],  # Peut être vide sur Linux
    logger=None
) -> Dict[str, int]:
    """
    Archivage complet : Incoming -> Processed + Nettoyage serveur SFTP
    
    Args:
        base_filename: Nom de base sans extension (ex: "client_20241201")
        archive_root: Racine archives (ex: "/data/sftp_cbmdata01/Processed")
        incoming_paths: Dict des chemins Incoming {type: path}
        sftp_server_paths: Dict des chemins SFTP serveur {type: path} (vide sur Linux)
        logger: Logger Prefect (optionnel)
    
    Returns:
        dict: {
            'archived_count': int,
            'cleaned_count': int,
            'total_files': int
        }
    """
    today = datetime.now().strftime("%Y-%m-%d")
    archive_dir = Path(archive_root) / today / "data"
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    archived_count = 0
    cleaned_count = 0
    
    # ========================================================================
    # ETAPE 1 : Archiver depuis Incoming -> Processed
    # ========================================================================
    
    for file_type, src_path in incoming_paths.items():
        src = Path(src_path)
        if src.exists():
            dst = archive_dir / src.name
            
            if safe_move(str(src), str(dst)):
                if logger:
                    logger.info(f"Archive : {src.name} -> {dst}")
                archived_count += 1
            else:
                if logger:
                    logger.warning(f"Impossible d'archiver : {src.name}")
        else:
            if logger:
                logger.debug(f"Fichier absent (skip) : {src.name}")
    
    # ========================================================================
    # ETAPE 2 : Nettoyer serveur SFTP
    # ========================================================================
    if sftp_server_paths:
        for file_type, sftp_path in sftp_server_paths.items():
            sftp_file = Path(sftp_path)
            if sftp_file.exists():
                try:
                    os.remove(sftp_file)
                    if logger:
                        logger.info(f"Supprime du serveur SFTP : {sftp_file.name}")
                    cleaned_count += 1
                    
                except PermissionError:
                    if logger:
                        logger.warning(f"Fichier verrouille sur serveur SFTP : {sftp_file.name}")
                        
                except Exception as e:
                    if logger:
                        logger.warning(f"Impossible de supprimer {sftp_file.name} : {e}")
            else:
                if logger:
                    logger.debug(f"Fichier deja absent du serveur SFTP : {sftp_file.name}")
    
    # ========================================================================
    # RESUME
    # ========================================================================
    
    total_files = len(incoming_paths)
    
    if logger:
        logger.info(
            f"Archivage termine : "
            f"{archived_count}/{total_files} archives, "
            f"{cleaned_count}/{total_files} nettoyes sur serveur SFTP"
        )
    
    return {
        'archived_count': archived_count,
        'cleaned_count': cleaned_count,
        'total_files': total_files
    }