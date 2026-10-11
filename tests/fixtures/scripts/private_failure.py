from vectorops.runner import diagnostics


async def main(ctx: object) -> None:
    diagnostics.log("custom.event", fields={"password": "private-input"})
    raise ValueError("private-failure")
