SET search_path = auto, pg_catalog;
BEGIN;

CREATE TABLE counter (
	uid text NOT NULL,
	referer text DEFAULT '-' NOT NULL,
	ip inet NOT NULL,
	day date DEFAULT now() NOT NULL,
	count integer DEFAULT 0 NOT NULL
);

CREATE TABLE counter_sum (
	referer text DEFAULT '-'::text NOT NULL,
	day date DEFAULT now() NOT NULL,
	count integer DEFAULT 0 NOT NULL
);

CREATE or replace FUNCTION get_stats(target text) RETURNS record LANGUAGE sql AS $_$
select 
	coalesce((select count from counter_sum where day = current_date and referer = $1), 0) as today,
	coalesce((select count from counter_sum where day = (current_date - interval '1 day')::date and referer = $1), 0) as lastday,
	coalesce((select sum(count) from counter_sum
		where date_trunc('week', current_date) = date_trunc('week', day) and day <> current_date and referer = $1
		group by referer), 0) as week,
	coalesce((select sum(count) from counter_sum where day <> current_date and referer = $1 group by referer), 0) as whole
$_$;

CREATE FUNCTION counter_sum__change_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
declare
	delta_count smallint;
begin
	if (TG_OP = 'UPDATE') then
		if (old.referer <> new.referer or old.day <> new.day) then
			raise exception 'Changing referer or day is prohibited';
		end if;
		delta_count = new.count - old.count;
	elsif (TG_OP = 'INSERT') then
		delta_count = new.count;
	elsif (TG_OP = 'DELETE') then
		raise exception 'Deleting counters prohibited';
	end if;
	<<insert_update>> loop
		update counter_sum set count = count + delta_count
			where referer = new.referer and day = new.day;
		exit insert_update when found;
		begin
			insert into counter_sum (referer, day, count) values (new.referer, new.day, delta_count);
			exit insert_update;
			exception when unique_violation then
		end;
	end loop insert_update;
	return new;
end; $$;

CREATE FUNCTION merge_counter(merge_uid text, merge_referer text, merge_ip inet, merge_count smallint) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
	<<insert_update>> LOOP
		UPDATE counter SET count = count + merge_count
			WHERE uid = merge_uid and ip = merge_ip and referer = merge_referer and day = current_date;
		exit insert_update when found;
		BEGIN
			INSERT INTO counter (uid, referer, ip, count) VALUES (merge_uid, merge_referer, merge_ip, merge_count);
			EXIT insert_update;
			EXCEPTION WHEN unique_violation THEN
		END;
	END LOOP insert_update;
	return true;
END;
$$;

CREATE INDEX counter__date ON counter USING btree (day);
CREATE UNIQUE INDEX counter__uid_nreferer_ip_date ON counter (uid, ip, day) WHERE (referer IS NULL);
CREATE UNIQUE INDEX counter__uid_referer_ip_date ON counter (uid, referer, ip, day);
create unique index counter_sum__referer_day ON counter_sum (referer, day);
CREATE TRIGGER counter__change AFTER INSERT OR DELETE OR UPDATE ON counter FOR EACH ROW EXECUTE PROCEDURE counter_sum__change_trigger();

end;
