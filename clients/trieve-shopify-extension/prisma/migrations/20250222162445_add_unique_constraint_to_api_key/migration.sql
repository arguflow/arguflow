/*
  Warnings:

  - A unique constraint covering the columns `[userId,shop]` on the table `ApiKey` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "ApiKey_userId_shop_key" ON "ApiKey"("userId", "shop");
