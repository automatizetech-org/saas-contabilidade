import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { SefazLoginsField } from "@/components/companies/SefazLoginsField";
import type { CompanySefazLogin } from "@/services/companiesService";
import type { Json } from "@/types/database";
import {
  getRobotConfigFieldDefaultValue,
  getUniqueCityOptions,
  isRobotFieldVisible,
  type RobotConfigFieldSchema,
  type RobotConfigTarget,
} from "@/lib/robotConfigSchemas";

type TargetValues = Partial<Record<RobotConfigTarget, Record<string, Json>>>;
type CompanyOption = {
  id: string;
  name: string;
  document: string | null;
};

function normalizeDigits(value: string | null | undefined): string {
  return String(value ?? "").replace(/\D/g, "");
}

export function RobotConfigFieldGroup({
  fields,
  valuesByTarget,
  onChangeField,
  globalLogins,
  onChangeGlobalLogins,
  cityNames = [],
  companies = [],
  disabled = false,
  loginLabelByCpf,
}: {
  fields: RobotConfigFieldSchema[];
  valuesByTarget: TargetValues;
  onChangeField: (target: RobotConfigTarget, key: string, value: Json) => void;
  globalLogins?: CompanySefazLogin[];
  onChangeGlobalLogins?: (next: CompanySefazLogin[]) => void;
  cityNames?: Array<string | null | undefined>;
  companies?: CompanyOption[];
  disabled?: boolean;
  loginLabelByCpf?: Map<string, string>;
}) {
  if (fields.length === 0) return null;

  const cityOptions = getUniqueCityOptions(cityNames);

  return (
    <div className="space-y-3">
      {fields
        .filter((field) => isRobotFieldVisible(field, valuesByTarget))
        .map((field) => {
          const target = field.target ?? "company_settings";
          const currentValue =
            target === "global_logins"
              ? globalLogins ?? []
              : (valuesByTarget[target]?.[field.key] ?? getRobotConfigFieldDefaultValue(field));

          if (field.type === "login_list") {
            if (!onChangeGlobalLogins) return null;
            return (
              <SefazLoginsField
                key={`${target}:${field.key}`}
                value={globalLogins ?? []}
                onChange={onChangeGlobalLogins}
                disabled={disabled}
                title={field.label}
                description={field.help_text}
                defaultLabel="Login padrão do robô"
              />
            );
          }

          if (field.type === "boolean") {
            return (
              <div key={`${target}:${field.key}`} className="flex items-start gap-3 rounded-lg border border-border bg-muted/20 p-3">
                <Checkbox
                  checked={Boolean(currentValue)}
                  onCheckedChange={(checked) => onChangeField(target, field.key, checked === true)}
                  disabled={disabled}
                  id={`${target}-${field.key}`}
                />
                <div className="space-y-1">
                  <Label htmlFor={`${target}-${field.key}`}>{field.label}</Label>
                  {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
                </div>
              </div>
            );
          }

          if (field.type === "auth_mode") {
            const value = String(currentValue || "password");
            return (
              <div key={`${target}:${field.key}`} className="space-y-2">
                <Label>{field.label}</Label>
                <Select
                  value={value}
                  onValueChange={(next) => onChangeField(target, field.key, next)}
                  disabled={disabled}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="password">Login</SelectItem>
                    <SelectItem value="certificate">Certificado</SelectItem>
                  </SelectContent>
                </Select>
                {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
              </div>
            );
          }

          if (field.type === "login_binding") {
            const loginOptions = globalLogins ?? [];
            return (
              <div key={`${target}:${field.key}`} className="space-y-2 rounded-md border border-border bg-background/60 p-3">
                <Label>{field.label}</Label>
                <Select
                  value={String(currentValue || "__none__")}
                  onValueChange={(next) => onChangeField(target, field.key, next === "__none__" ? null : next)}
                  disabled={disabled || loginOptions.length === 0}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={loginOptions.length === 0 ? "Cadastre logins no editar robô" : "Selecione o login"} />
                  </SelectTrigger>
                  <SelectContent>
                    {loginOptions.length === 0 ? (
                      <SelectItem value="__none__">Nenhum login global cadastrado</SelectItem>
                    ) : (
                      loginOptions.map((login) => (
                        <SelectItem key={login.cpf} value={login.cpf}>
                          {login.cpf}
                          {loginLabelByCpf?.get(login.cpf) ? ` • ${loginLabelByCpf.get(login.cpf)}` : ""}
                          {login.is_default ? " • padrão" : ""}
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
                {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
              </div>
            );
          }

          if (field.type === "select" || field.type === "city_select") {
            const options = field.type === "city_select" && (field.options?.length ?? 0) === 0
              ? cityOptions
              : (field.options ?? []);
            return (
              <div key={`${target}:${field.key}`} className="space-y-2">
                <Label>{field.label}</Label>
                <Select
                  value={String(currentValue || "__empty__")}
                  onValueChange={(next) => onChangeField(target, field.key, next === "__empty__" ? "" : next)}
                  disabled={disabled}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={field.placeholder || "Selecione"} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__empty__">Sem filtro</SelectItem>
                    {options.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
              </div>
            );
          }

          if (field.type === "company_select") {
            const options = companies.filter((company) => {
              if (field.company_option_filter === "valid_cnpj") {
                return normalizeDigits(company.document).length === 14;
              }
              return true;
            });

            return (
              <div key={`${target}:${field.key}`} className="space-y-2">
                <Label>{field.label}</Label>
                <Select
                  value={String(currentValue || "__empty__")}
                  onValueChange={(next) => onChangeField(target, field.key, next === "__empty__" ? "" : next)}
                  disabled={disabled}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={field.placeholder || "Selecione"} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__empty__">Sem selecao</SelectItem>
                    {options.map((company) => (
                      <SelectItem key={company.id} value={company.id}>
                        {company.name}
                        {company.document ? ` • ${company.document}` : ""}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
              </div>
            );
          }

          if (field.type === "textarea") {
            return (
              <div key={`${target}:${field.key}`} className="space-y-2">
                <Label>{field.label}</Label>
                <Textarea
                  value={String(currentValue ?? "")}
                  onChange={(event) => onChangeField(target, field.key, event.target.value)}
                  placeholder={field.placeholder}
                  disabled={disabled}
                />
                {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
              </div>
            );
          }

          if (field.type === "number") {
            return (
              <div key={`${target}:${field.key}`} className="space-y-2">
                <Label>{field.label}</Label>
                <Input
                  type="number"
                  value={currentValue === null || currentValue === undefined ? "" : String(currentValue)}
                  onChange={(event) => {
                    const nextValue = event.target.value;
                    onChangeField(target, field.key, nextValue === "" ? "" : Number(nextValue));
                  }}
                  placeholder={field.placeholder}
                  disabled={disabled}
                />
                {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
              </div>
            );
          }

          return (
            <div key={`${target}:${field.key}`} className="space-y-2">
              <Label>{field.label}</Label>
              <Input
                type={field.type === "password" ? "password" : "text"}
                value={String(currentValue ?? "")}
                onChange={(event) => onChangeField(target, field.key, event.target.value)}
                placeholder={field.placeholder}
                disabled={disabled}
                autoComplete="off"
              />
              {field.help_text ? <p className="text-[10px] text-muted-foreground">{field.help_text}</p> : null}
            </div>
          );
        })}
    </div>
  );
}
