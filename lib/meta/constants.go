package meta

const mappingIndex = `{
    "mappings": {
        "meta": {
            "properties": {
                "tagsNested": {
                    "type": "nested",
                    "properties": {
                        "tagKey": {
                            "type": "string"
                        },
                        "tagValue": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "metatext": {
            "properties": {
                "tagsNested": {
                    "type": "nested",
                    "properties": {
                        "tagKey": {
                            "type": "string"
                        },
                        "tagValue": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }
}`
