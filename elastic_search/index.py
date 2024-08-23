import random
import string
import time

from elasticsearch import Elasticsearch, helpers


class Index:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")

    def enter_key(self, exec_method: str):
        match exec_method:
            case "create_compare_dataset":
                # 10万件のデータを生成
                self.generate_large_dataset_with_batches("large_dataset", 100000)
                # 100件のデータを生成
                self.generate_dataset("small_dataset", 100)
            case "generate_compare_text_dataset":
                self.generate_compare_text_dataset()
            case "generate_double_text_dataset":
                self.generate_double_text_dataset()
            case "deletes":
                self.deletes()
            case "counts":
                self.counts()
            case "search":
                self.search()
            case _:
                print("no method selected")

    def generate_random_text(self, length=10):
        letters = string.ascii_lowercase
        return "".join(random.choice(letters) for i in range(length))

    def create_index_if_not_exists(self, index_name):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(
                index=index_name,
                body={
                    "settings": {"number_of_shards": 1, "number_of_replicas": 1},
                    "mappings": {"properties": {"content": {"type": "text"}}},
                },
            )
            print(f"Index '{index_name}' created.")

    def generate_dataset(self, index_name, num_records):
        # インデックスが存在しない場合は作成
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name)

        # インデックスするデータを準備
        actions = [
            {
                "_index": index_name,
                "_source": {
                    "content": f"サンプルテキスト {i} ランダム文字 {self.generate_random_text()}"
                },
            }
            for i in range(num_records)
        ]

        # データを一括インデックス
        helpers.bulk(self.es, actions)

    def generate_large_dataset_with_batches(
        self, index_name, num_records, batch_size=1000
    ):
        # インデックスが存在しない場合は作成
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name)

        current_count = self.es.count(index=index_name)["count"]
        remaining_records = num_records - current_count

        if remaining_records <= 0:
            print(
                f"The index '{index_name}' already contains {num_records} or more documents."
            )
            return

        # バッチ処理でデータをインデックス
        for start in range(current_count, num_records, batch_size):
            end = min(start + batch_size, num_records)
            actions = [
                {
                    "_index": index_name,
                    "_source": {
                        "content": f"サンプルテキスト {i} ランダム文字 {self.generate_random_text()}"
                    },
                }
                for i in range(start, end)
            ]

            retries = 0
            max_retries = 3
            delay = 2
            while retries < max_retries:
                try:
                    helpers.bulk(self.es, actions)
                    print(f"Indexed documents from {start} to {end}")
                    break  # 成功した場合はループを抜ける
                except helpers.BulkIndexError as bulk_error:
                    print(
                        f"Bulk indexing error: {len(bulk_error.errors)} document(s) failed to index."
                    )
                    for error in bulk_error.errors:
                        if error["index"]["status"] == 429:
                            print("Disk usage exceeded flood-stage watermark.")
                            self.es.indices.put_settings(
                                index=index_name,
                                body={"index.blocks.read_only_allow_delete": False},
                            )
                            print("読み取り専用モードを解除しました。")
                        print(error)  # エラーの詳細を出力
                    retries += 1
                    if retries < max_retries:
                        print(
                            f"Retrying batch from {start} to {end} (Attempt {retries}/{max_retries})"
                        )
                        time.sleep(delay)
                    else:
                        print(
                            f"Failed to index batch from {start} to {end} after {max_retries} attempts"
                        )

    def generate_compare_text_dataset(self):
        # 大きなテキストのデータ
        large_text = "サンプルテキスト 1 " + "ランダム文字" * 100  # 1000文字に調整
        large_doc = {"content": large_text}

        # 小さなテキストのデータ
        small_text = "サンプルテキスト 1"  # 10文字に調整
        small_doc = {"content": small_text}

        # 大きなテキストをlarge_text_datasetにインデックス
        self.es.index(index="large_text_dataset", document=large_doc)

        # 小さなテキストをsmall_text_datasetにインデックス
        self.es.index(index="small_text_dataset", document=small_doc)

    def generate_double_text_dataset(self):
        # 重複文字が多いデータセットと少ないデータセットを作成
        high_duplication_text = "サンプルテキスト " * 10
        high_duplication_doc = {"content": high_duplication_text}

        low_duplication_text = "サンプルテキスト 1 " + "ランダム文字" * 8
        low_duplication_doc = {"content": low_duplication_text}

        self.es.index(index="high_duplication_dataset", document=high_duplication_doc)
        self.es.index(index="low_duplication_dataset", document=low_duplication_doc)

    def deletes(self):
        self.es.indices.delete(index="large_dataset")
        self.es.indices.delete(index="small_dataset")

    def counts(self):
        print(self.es.count(index="large_dataset")["count"])
        print(self.es.count(index="small_dataset")["count"])

    def search(self):
        large = self.es.search(index="large_dataset", body={"query": {"match_all": {}}})
        small = self.es.search(index="small_dataset", body={"query": {"match_all": {}}})
        print(f"large_dataset Got {large['hits']['total']['value']} hits")
        print(f"small_dataset Got {large['hits']['total']['value']} hits")
        for hit in small["hits"]["hits"]:
            print(f"ID: {hit['_id']}, Content: {hit['_source']['content']}")
        for hit in large["hits"]["hits"]:
            print(f"ID: {hit['_id']}, Content: {hit['_source']['content']}")
