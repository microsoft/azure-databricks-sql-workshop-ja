# Databricks SQL on Azure Workshop [Japanese]
Databricks SQL と Synapse パイプライン & データ フローを中心とする Microsoft Azure のデータ分析プラットフォームの構築のノウハウを学習するためのワークショップの資材管理用の GitHub リポジトリです。以下 URL よりワークショップを開始できます。

https://microsoft.github.io/azure-databricks-sql-workshop-ja/

## ワークショップのドキュメントの更新方法 (Contributor 向けの情報)
本ワークショップのドキュメントは Markdown 形式のファイル (`workshop.md`) を [claat (Google Codelabs command line tool)](https://github.com/googlecodelabs/tools/tree/main/claat) でレンダリングして作成しています。

以下の手順でドキュメントを更新します。

1. 以下ドキュメントを参考にして claat をセットアップする
    - https://zenn.dev/nakazax/articles/18506708b5eea9
2. `workshop.md` を更新する
3. ターミナルで `claat export workshop.md` を実行し `docs/` ディレクトリ配下のファイル群が更新されることを確認する
4. ターミナルで `claat serve docs/` を実行しレンダリング結果を確認する

## Contributing
This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks
This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
