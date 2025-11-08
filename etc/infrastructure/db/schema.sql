CREATE TABLE IF NOT EXISTS notes_raw(
  note_id UUID PRIMARY KEY,
  patient_id UUID,
  ts timestamptz,
  note_text text,
  source text
);

CREATE TABLE IF NOT EXISTS notes_extracted(
  note_id UUID REFERENCES notes_raw(note_id),
  symptoms text[],
  entities jsonb
);

CREATE TABLE IF NOT EXISTS notes_diagnosed(
  note_id UUID REFERENCES notes_raw(note_id),
  predictions jsonb, -- array of {condition, confidence}
  top_condition text,
  top_confidence double precision
);

CREATE TABLE IF NOT EXISTS notes_planned(
  note_id UUID REFERENCES notes_raw(note_id),
  plan jsonb,
  rationale text
);
