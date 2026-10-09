-- Retention support for ad_lock (AD_LOCK_RETENTION). settled_at is when a lock row left the 'locked' state
-- (released or closed); only settled rows are ever pruned. A trigger sets it so no writer has to remember to.
ALTER TABLE ad_lock ADD COLUMN settled_at timestamptz;
UPDATE ad_lock SET settled_at = created_at WHERE state <> 'locked';

CREATE FUNCTION ad_lock_settle() RETURNS trigger AS $$
BEGIN
  IF NEW.state <> 'locked' AND OLD.state = 'locked' THEN
    NEW.settled_at := now();
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ad_lock_settle BEFORE UPDATE OF state ON ad_lock
  FOR EACH ROW EXECUTE FUNCTION ad_lock_settle();

CREATE INDEX ad_lock_settled_at ON ad_lock (settled_at) WHERE settled_at IS NOT NULL;
