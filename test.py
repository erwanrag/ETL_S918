import os
import time
import shutil

SRC = r"C:\ProgramData\ssh\SFTPRoot\Incoming\db_metadata\test.txt"
DST = r"C:\ProgramData\ssh\SFTPRoot\Processed\test.txt"


def wait_until_file_released(path, timeout=60):
    """
    Attend que Windows/OpenSSH libère le fichier.
    """
    start = time.time()
    print(f"[INFO] Waiting for file to be released: {path}")

    while True:
        try:
            with open(path, "rb"):
                print("[INFO] File released ✓")
                return True
        except PermissionError:
            if time.time() - start > timeout:
                print("[ERROR] Timeout: file never released ❌")
                return False
            time.sleep(0.5)


def safe_move(src, dst):
    """
    Move robuste pour Windows/OpenSSH/SFTP.
    """
    print("=== TEST ARCHIVE ===")
    print(f"SRC = {src}")
    print(f"DST = {dst}")

    if not os.path.exists(src):
        print("[ERROR] Source file does NOT exist ❌")
        return False

    # 1) Attendre libération par OpenSSH
    ok = wait_until_file_released(src)
    if not ok:
        print("[ERROR] File never became free ❌")
        return False

    # 2) Essayer MOVE
    for i in range(10):
        try:
            shutil.move(src, dst)
            print("[MOVE] Success ✓")
            return True
        except PermissionError:
            print(f"[MOVE] Permission denied (attempt {i+1}/10)… retrying")
            time.sleep(1)
        except Exception as e:
            print(f"[MOVE] Failed ❌ : {e}")
            break

    # 3) Fallback COPY → DELETE
    print("[FALLBACK] Using COPY → DELETE method")

    try:
        shutil.copy2(src, dst)
        print("[COPY] OK ✓")
    except Exception as e:
        print(f"[COPY] Failed ❌ : {e}")
        return False

    time.sleep(2)

    try:
        os.remove(src)
        print("[DELETE] OK ✓")
        return True
    except Exception as e:
        print(f"[DELETE] Failed ❌ : {e}")
        return False


# ---- RUN TEST ----
safe_move(SRC, DST)
