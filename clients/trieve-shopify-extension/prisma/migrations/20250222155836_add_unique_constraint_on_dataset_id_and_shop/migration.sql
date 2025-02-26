/*
  Warnings:

  - A unique constraint covering the columns `[datasetId,shop]` on the table `CrawlSettings` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "CrawlSettings_datasetId_shop_key" ON "CrawlSettings"("datasetId", "shop");
