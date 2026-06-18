class ProcessedRanges:
    def __init__(self, ranges=None):
        self._ranges = []
        if ranges:
            for start, end in ranges:
                self.add_range(start, end)

    @classmethod
    def from_string(cls, value):
        ranges = cls()
        if value is None or value == "":
            return ranges

        for part in value.split(";"):
            if not part:
                continue
            if "-" in part:
                start, end = part.split("-", 1)
                ranges.add_range(int(start), int(end))
            else:
                ranges.add(int(part))
        return ranges

    def to_string(self):
        parts = []
        for start, end in self._ranges:
            if start == end:
                parts.append(str(start))
            else:
                parts.append(f"{start}-{end}")
        return ";".join(parts)

    def contains(self, message_id):
        message_id = int(message_id)
        for start, end in self._ranges:
            if message_id < start:
                return False
            if start <= message_id <= end:
                return True
        return False

    def add(self, message_id):
        return self.add_range(message_id, message_id)

    def add_range(self, start, end):
        start = int(start)
        end = int(end)
        if start > end:
            raise ValueError("range start must be less than or equal to end")

        new_start = start
        new_end = end
        merged = []
        inserted = False

        for current_start, current_end in self._ranges:
            if current_end + 1 < new_start:
                merged.append((current_start, current_end))
            elif new_end + 1 < current_start:
                if not inserted:
                    merged.append((new_start, new_end))
                    inserted = True
                merged.append((current_start, current_end))
            else:
                new_start = min(new_start, current_start)
                new_end = max(new_end, current_end)

        if not inserted:
            merged.append((new_start, new_end))

        self._ranges = merged
        return self

    def as_tuples(self):
        return tuple(self._ranges)
