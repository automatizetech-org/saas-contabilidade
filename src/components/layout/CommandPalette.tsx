import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  LayoutDashboard,
  FileText,
  Users,
  DollarSign,
  Settings,
  Activity,
  FileSpreadsheet,
  Shield,
  Landmark,
} from "lucide-react";
import { LionIcon } from "@/components/icons/LionIcon";

const pages = [
  { name: "Dashboard", path: "/dashboard", icon: LayoutDashboard, group: "Navegação" },
  { name: "Fiscal", path: "/fiscal", icon: FileText, group: "Navegação" },
  { name: "Fiscal - NFS", path: "/fiscal/nfs", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - NFE/NFC", path: "/fiscal/nfe-nfc", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - DIFAL", path: "/fiscal/difal", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - IRRF/CSLL", path: "/fiscal/irrf-csll", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - Certidões", path: "/fiscal/certidoes", icon: FileText, group: "Fiscal" },
  { name: "Departamento Pessoal", path: "/dp", icon: Users, group: "Navegação" },
  { name: "Inteligência Tributária", path: "/inteligencia-tributaria", icon: Landmark, group: "Navegação" },
  { name: "Tributária - Simples Nacional", path: "/inteligencia-tributaria/simples-nacional", icon: Landmark, group: "Inteligência Tributária" },
  { name: "Tributária - Lucro Real", path: "/inteligencia-tributaria/lucro-real", icon: Landmark, group: "Inteligência Tributária" },
  { name: "Tributária - Lucro Presumido", path: "/inteligencia-tributaria/lucro-presumido", icon: Landmark, group: "Inteligência Tributária" },
  { name: "IR", path: "/ir", icon: LionIcon, group: "Navegação" },
  { name: "Paralegal", path: "/paralegal", icon: Shield, group: "Navegação" },
  { name: "Paralegal - Certificados", path: "/paralegal/certificados", icon: Shield, group: "Paralegal" },
  { name: "Paralegal - Tarefas", path: "/paralegal/tarefas", icon: Shield, group: "Paralegal" },
  { name: "Paralegal - Clientes", path: "/paralegal/clientes", icon: Shield, group: "Paralegal" },
  { name: "Financeiro", path: "/financeiro", icon: DollarSign, group: "Navegação" },
  { name: "Operações", path: "/operacoes", icon: Activity, group: "Navegação" },
  { name: "Documentos", path: "/documentos", icon: FileSpreadsheet, group: "Navegação" },
  { name: "Administração", path: "/admin", icon: Settings, group: "Sistema" },
];

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, []);

  const groups = [...new Set(pages.map((page) => page.group))];

  return (
    <CommandDialog open={open} onOpenChange={setOpen}>
      <CommandInput placeholder="Buscar páginas, empresas, documentos..." />
      <CommandList>
        <CommandEmpty>Nenhum resultado encontrado.</CommandEmpty>
        {groups.map((group) => (
          <CommandGroup key={group} heading={group}>
            {pages
              .filter((page) => page.group === group)
              .map((page) => (
                <CommandItem
                  key={page.path}
                  onSelect={() => {
                    navigate(page.path);
                    setOpen(false);
                  }}
                >
                  <page.icon className="mr-2 h-4 w-4" />
                  <span>{page.name}</span>
                </CommandItem>
              ))}
          </CommandGroup>
        ))}
      </CommandList>
    </CommandDialog>
  );
}
