import { DOC_GROUPS, DOC_SECTIONS } from "../docs";

const groupLabelId = (group: string): string =>
  `docs-nav-${group.toLowerCase().replaceAll(" ", "-")}`;

interface DocsNavProps {
  activeSectionId: string;
  onSelectSection: (sectionId: string) => void;
}

const DocsNav = ({ activeSectionId, onSelectSection }: DocsNavProps) => (
  <nav className="flex flex-col gap-4">
    {DOC_GROUPS.map((group) => (
      <div
        key={group}
        role="group"
        aria-labelledby={groupLabelId(group)}
        className="flex flex-col gap-0.5"
      >
        <p
          id={groupLabelId(group)}
          className="px-3 pb-1 text-xs font-bold uppercase tracking-wider text-accent"
        >
          {group}
        </p>
        {DOC_SECTIONS.filter((section) => section.group === group).map(
          (section) => (
            <button
              key={section.id}
              aria-current={activeSectionId === section.id ? "page" : undefined}
              onClick={() => {
                onSelectSection(section.id);
              }}
              className={`text-left text-sm px-3 py-2 rounded-md transition ${
                activeSectionId === section.id
                  ? "bg-accent-muted text-white font-medium"
                  : "text-slate-400 hover:text-white hover:bg-surface-hover"
              }`}
            >
              {section.title}
            </button>
          ),
        )}
      </div>
    ))}
  </nav>
);

export default DocsNav;
