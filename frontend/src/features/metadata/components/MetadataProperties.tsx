import JSONbig from "json-bigint";
import MetadataJson from "./MetadataJson";
import CopyIconButton from "../../../components/CopyIconButton";
import {
  METADATA_SECTION_BODY_CLASS,
  METADATA_SUMMARY_CLASS,
} from "../metadataStyles";

interface MetadataPropertiesProps {
  properties: Record<string, unknown> | undefined;
}
const jsonParser = JSONbig({ storeAsString: true });

const prettyPrintJsonTokens = (value: string): string => {
  let output = "";
  let indent = 0;
  let inString = false;
  let escaped = false;
  const newline = (): void => {
    output = `${output.trimEnd()}\n${"  ".repeat(indent)}`;
  };
  for (const character of value.trim()) {
    if (inString) {
      output += character;
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') {
      inString = true;
      output += character;
    } else if (character === "{" || character === "[") {
      output += character;
      indent += 1;
      newline();
    } else if (character === "}" || character === "]") {
      indent -= 1;
      const trimmed = output.trimEnd();
      if (trimmed.endsWith("{") || trimmed.endsWith("[")) {
        output = `${trimmed}${character}`;
      } else {
        output = `${trimmed}\n${"  ".repeat(indent)}${character}`;
      }
    } else if (character === ",") {
      output += character;
      newline();
    } else if (character === ":") {
      output += ": ";
    } else if (!/\s/.test(character)) {
      output += character;
    }
  }
  return output;
};

const formattedJson = (value: unknown): string | null => {
  if (typeof value === "object" && value !== null)
    return JSON.stringify(value, null, 2);
  if (typeof value !== "string" || !/^\s*(?:\[|\{)/.test(value)) return null;
  try {
    const parsed: unknown = jsonParser.parse(value);
    return typeof parsed === "object" && parsed !== null
      ? prettyPrintJsonTokens(value)
      : null;
  } catch {
    return null;
  }
};

const MetadataProperties = ({ properties }: MetadataPropertiesProps) => {
  const entries = Object.entries(properties ?? {});
  return (
    <details className="border-t border-edge">
      <summary className={METADATA_SUMMARY_CLASS}>
        Table properties{" "}
        <span className="ml-2 text-xs font-normal text-slate-400">
          {entries.length} {entries.length === 1 ? "property" : "properties"}
        </span>
      </summary>
      <div className={`${METADATA_SECTION_BODY_CLASS} px-5 py-2`}>
        {entries.length === 0 && (
          <p className="text-sm text-slate-400">
            No table properties recorded.
          </p>
        )}
        {entries.map(([key, value]) => {
          const json = formattedJson(value);
          const plain: string | undefined =
            typeof value === "string"
              ? value
              : value === undefined
                ? undefined
                : JSON.stringify(value);
          return (
            <div
              key={key}
              className="grid gap-3 border-t border-edge py-3 first:border-t-0 md:grid-cols-[minmax(12rem,1fr)_minmax(0,2fr)]"
            >
              <code className="text-xs break-words text-accent-text">
                {key}
              </code>
              {json !== null ? (
                <details
                  className="min-w-0"
                  open={json.split("\n").length <= 8}
                >
                  <summary className="mb-2 cursor-pointer text-xs text-slate-400">
                    JSON value ({json.split("\n").length} lines)
                  </summary>
                  <MetadataJson
                    text={json}
                    {...(plain === undefined ? {} : { copyText: plain })}
                    label={key}
                  />
                </details>
              ) : (
                <div className="flex min-w-0 items-start gap-2">
                  <span className="min-w-0 flex-1 break-all text-sm text-slate-300">
                    {plain ?? "Unavailable"}
                  </span>
                  {plain !== undefined && (
                    <CopyIconButton text={plain} title={`Copy ${key}`} />
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </details>
  );
};
export default MetadataProperties;
