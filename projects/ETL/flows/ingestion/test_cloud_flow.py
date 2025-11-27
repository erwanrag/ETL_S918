from prefect import flow

@flow
def test_cloud():
    print("Flow Prefect Cloud OK !")

if __name__ == "__main__":
    test_cloud()
