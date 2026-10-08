-- ad_ref.status was never kept current and nothing reads it; the live status always comes from ad.GetAd.
ALTER TABLE ad_ref DROP COLUMN status;
