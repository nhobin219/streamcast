// A streamcast client in plain JavaScript: Node's built-in `WebSocket`, which
// is the WHATWG API every browser exposes, and the reader from docs/SPEC.md §2
// ("Reading a row in another language"), taken from the SPEC verbatim so the
// documented code is the tested code.
//
//   node client.mjs <ws://host:port/stream> <path/to/SPEC.md> <row to publish, as JSON>
//
// Subscribes from offset 1, reads two replayed rows, publishes one row as a
// JSON publisher would (binary as text in its column's encoding), reads it
// back live, and prints what it saw as one JSON object. Bytes are printed as
// arrays of integers so the Python side can compare them exactly.
import fs from "node:fs";

const [, , url, specPath, published] = process.argv;
const spec = fs.readFileSync(specPath, "utf8");
const source = spec.match(/```js\n([\s\S]*?)```/)[1];
const read = new Function(`${source}\nreturn read;`)();

const plain = (v) =>
  v instanceof Uint8Array ? Array.from(v)
  : Array.isArray(v) ? v.map(plain)
  : v !== null && typeof v === "object" ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, plain(x)]))
  : v;

const out = { textFrames: true, rows: [], ack: null };
let schema;

const done = () => {
  if (out.rows.length === 3 && out.ack !== null) {
    console.log(JSON.stringify(out));
    process.exit(0);
  }
};

setTimeout(() => {
  console.log(JSON.stringify({ error: "timed out", ...out }));
  process.exit(2);
}, 20_000);

const publish = () => {
  const publisher = new WebSocket(`${url}?publish`);
  let greeted = false;
  publisher.onmessage = (event) => {
    if (typeof event.data !== "string") out.textFrames = false;
    if (!greeted) {
      greeted = true;
      publisher.send(published);
      return;
    }
    out.ack = JSON.parse(event.data);
    publisher.close();
    done();
  };
};

const subscriber = new WebSocket(`${url}?offset=1`);
subscriber.onmessage = (event) => {
  // A browser hands a text frame over as a string, and a binary one as a Blob.
  if (typeof event.data !== "string") out.textFrames = false;
  const data = JSON.parse(event.data);
  if (schema === undefined) {
    schema = data.schema;
    return;
  }
  const [offset, msg] = data;
  out.rows.push([offset, plain(read(schema, msg))]);
  if (out.rows.length === 2) publish();
  done();
};
