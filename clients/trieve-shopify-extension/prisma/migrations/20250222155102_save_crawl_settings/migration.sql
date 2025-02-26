-- CreateTable
CREATE TABLE "CrawlSettings" (
    "id" TEXT NOT NULL,
    "datasetId" UUID,
    "shop" TEXT NOT NULL DEFAULT '',
    "crawlSettings" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CrawlSettings_pkey" PRIMARY KEY ("id")
);
