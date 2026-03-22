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
  Activity,
  DollarSign,
  FileSpreadsheet,
  FileText,
  Landmark,
  LayoutDashboard,
  Settings,
  Shield,
  Users,
} from "lucide-react";
import { LionIcon } from "@/components/icons/LionIcon";

const pages = [
  { name: "Dashboard", path: "/dashboard", icon: LayoutDashboard, group: "Navegacao" },
  { name: "Fiscal", path: "/fiscal", icon: FileText, group: "Navegacao" },
  { name: "Fiscal - NFS", path: "/fiscal/nfs", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - NFE/NFC", path: "/fiscal/nfe-nfc", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - Declaracoes", path: "/fiscal/declaracoes", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - DIFAL", path: "/fiscal/difal", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - IRRF/CSLL", path: "/fiscal/irrf-csll", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - Certidoes", path: "/fiscal/certidoes", icon: FileText, group: "Fiscal" },
  { name: "Fiscal - Caixa Postal E-CAC", path: "/fiscal/caixa-postal-ecac", icon: FileText, group: "Fiscal" },
  { name: "Departamento Pessoal", path: "/dp", icon: Users, group: "Navegacao" },
  { name: "Inteligencia Tributaria", path: "/inteligencia-tributaria", icon: Landmark, group: "Navegacao" },
  { name: "Tributaria - Simples Nacional", path: "/inteligencia-tributaria/simples-nacional", icon: Landmark, group: "Inteligencia Tributaria" },
  { name: "Tributaria - Lucro Real", path: "/inteligencia-tributaria/lucro-real", icon: Landmark, group: "Inteligencia Tributaria" },
  { name: "Tributaria - Lucro Presumido", path: "/inteligencia-tributaria/lucro-presumido", icon: Landmark, group: "Inteligencia Tributaria" },
  { name: "IR", path: "/ir", icon: LionIcon, group: "Navegacao" },
  { name: "Paralegal", path: "/paralegal", icon: Shield, group: "Navegacao" },
  { name: "Paralegal - Certificados", path: "/paralegal/certificados", icon: Shield, group: "Paralegal" },
  { name: "Paralegal - Tarefas", path: "/paralegal/tarefas", icon: Shield, group: "Paralegal" },
  { name: "Paralegal - Clientes", path: "/paralegal/clientes", icon: Shield, group: "Paralegal" },
  { name: "Financeiro", path: "/financeiro", icon: DollarSign, group: "Navegacao" },
  { name: "Operacoes", path: "/operacoes", icon: Activity, group: "Navegacao" },
  { name: "Documentos", path: "/documentos", icon: FileSpreadsheet, group: "Navegacao" },
  { name: "Administracao", path: "/admin", icon: Settings, group: "Sistema" },
];

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    const down = (event: KeyboardEvent) => {
      if (event.key === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setOpen((current) => !current);
      }
    };

    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, []);

  const groups = [...new Set(pages.map((page) => page.group))];

  return (
    <CommandDialog open={open} onOpenChange={setOpen}>
      <CommandInput placeholder="Buscar paginas, empresas, documentos..." />
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
