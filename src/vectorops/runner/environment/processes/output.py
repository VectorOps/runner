"""Bounded byte-cursor output storage independent of backend transports."""

from collections import deque

from . import models as m


class OutputBuffer:
    def __init__(self, limit: int) -> None:
        if limit <= 0:
            raise ValueError("output limit must be positive")
        self.limit = limit
        self.start = 0
        self.end = 0
        self._chunks: deque[m.OutputChunk] = deque()

    def append(self, chunk: m.OutputChunk) -> None:
        if not chunk.data:
            return
        self._chunks.append(chunk)
        self.end += len(chunk.data)
        while self.end - self.start > self.limit:
            first = self._chunks.popleft()
            removed = min(len(first.data), self.end - self.start - self.limit)
            self.start += removed
            if removed < len(first.data):
                self._chunks.appendleft(
                    m.OutputChunk(stream=first.stream, data=first.data[removed:])
                )

    def read(self, cursor: int, limit: int) -> tuple[list[m.OutputChunk], int, int]:
        if cursor < 0 or cursor > self.end or limit <= 0:
            raise ValueError("invalid output cursor or limit")
        dropped = max(0, self.start - cursor)
        cursor = max(cursor, self.start)
        position = self.start
        result: list[m.OutputChunk] = []
        for chunk in self._chunks:
            end = position + len(chunk.data)
            if end > cursor and limit:
                data = chunk.data[max(0, cursor - position) :][:limit]
                result.append(m.OutputChunk(stream=chunk.stream, data=data))
                cursor += len(data)
                limit -= len(data)
            position = end
        return result, cursor, dropped
