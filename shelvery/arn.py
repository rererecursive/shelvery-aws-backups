

class ARN():
    """Convert a string that represents an ARN into its properties.
    """
    def __init__(self, arn, is_shelvery=False):
        tokens = arn.split(':')
        self.arn = arn
        self.header = ":".join(tokens[:2])
        self.resource = tokens[2]
        self.region = tokens[3]
        self.account_id = tokens[4]
        self.name = ":".join(tokens[6:])

        if is_shelvery:
            # Add some extra fields.
            name_tokens = self.name.split('-')
            self.retention_type = name_tokens[-1]
            self.date = name_tokens[-5:][:-1]   # e.g. 2017-12-01-0100
