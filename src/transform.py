"""Final transformation step. Validation has already normalized UTC and money."""


def transform(state: dict) -> dict:
    # txn_date was derived after UTC conversion during validation. Copy records so
    # later publish code cannot mutate validation state unexpectedly.
    state["transformed_transactions"] = [dict(row) for row in state["clean_transactions"]]
    return {"row_count": len(state["transformed_transactions"])}
