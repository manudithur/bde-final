-- Fix trips table to allow NULL shape_id after import
-- This allows trips without shapes to be imported
-- Run this AFTER gtfs-to-sql import if you get constraint errors

-- Drop the constraint that requires valid shape_id
DO $$
BEGIN
    -- Check if constraint exists and drop it
    IF EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'valid_shape_id' 
        AND conrelid = 'trips'::regclass
    ) THEN
        ALTER TABLE trips DROP CONSTRAINT valid_shape_id;
        RAISE NOTICE 'Dropped valid_shape_id constraint';
    ELSE
        RAISE NOTICE 'valid_shape_id constraint does not exist';
    END IF;
END $$;

-- Update empty shape_id strings to NULL
UPDATE trips 
SET shape_id = NULL 
WHERE shape_id = '' OR shape_id IS NULL;

-- Create a new constraint that allows NULL but validates non-NULL values
DO $$
BEGIN
    -- Only create constraint if shapes table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'shapes') THEN
        ALTER TABLE trips 
        ADD CONSTRAINT valid_shape_id 
        CHECK (shape_id IS NULL OR shape_id IN (SELECT shape_id FROM shapes));
        RAISE NOTICE 'Created new valid_shape_id constraint (allows NULL)';
    ELSE
        RAISE NOTICE 'Shapes table does not exist, skipping constraint creation';
    END IF;
END $$;

